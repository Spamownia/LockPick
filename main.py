import ftplib
import io
import re
import psycopg2
import requests

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# Konfiguracja bazy danych PostgreSQL Neon
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Regex do parsowania wpisów
LOG_PATTERN = re.compile(
    r"User: (?P<nick>.+?) \(.+?\)\. Success: (?P<success>Yes|No)\. "
    r"Elapsed time: (?P<time>[0-9.]+)\. Failed attempts: \d+\. "
    r"Target object: .+?\. Lock type: (?P<lock>.+?)\."
)

def connect_db():
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        sslmode=DB_CONFIG["sslmode"]
    )
    return conn

def create_tables(conn):
    with conn.cursor() as cur:
        # Tabela z historią przetworzonych plików
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_logs (
                filename TEXT PRIMARY KEY
            )
        """)
        # Tabela ze statystykami lockpicków
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpick_stats (
                nick TEXT,
                lock_type TEXT,
                attempts_total INTEGER,
                attempts_success INTEGER,
                attempts_fail INTEGER,
                time_sum FLOAT,
                PRIMARY KEY (nick, lock_type)
            )
        """)
    conn.commit()

def list_files_ftp(ftp):
    files = []
    lines = []
    ftp.retrlines('LIST', lines.append)
    for line in lines:
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            filename = parts[8]
            files.append(filename)
    return files

def is_log_processed(conn, filename):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM processed_logs WHERE filename=%s", (filename,))
        return cur.fetchone() is not None

def mark_log_processed(conn, filename):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO processed_logs(filename) VALUES(%s) ON CONFLICT DO NOTHING", (filename,))
    conn.commit()

def download_log_file(ftp, filename):
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    bio.seek(0)
    content_bytes = bio.read()
    # Dekodowanie UTF-16 LE
    content = content_bytes.decode('utf-16le')
    return content

def parse_log_content(content):
    entries = []
    for line in content.splitlines():
        if "[LogMinigame] [LockpickingMinigame_C]" in line:
            match = LOG_PATTERN.search(line)
            if match:
                nick = match.group("nick")
                success = match.group("success") == "Yes"
                time = float(match.group("time"))
                lock_type = match.group("lock")
                entries.append({
                    "nick": nick,
                    "success": success,
                    "time": time,
                    "lock_type": lock_type
                })
    return entries

def update_stats(conn, entries):
    with conn.cursor() as cur:
        for e in entries:
            # Pobierz aktualne statystyki
            cur.execute("""
                SELECT attempts_total, attempts_success, attempts_fail, time_sum
                FROM lockpick_stats
                WHERE nick=%s AND lock_type=%s
            """, (e["nick"], e["lock_type"]))
            row = cur.fetchone()
            if row:
                attempts_total, attempts_success, attempts_fail, time_sum = row
                attempts_total += 1
                attempts_success += 1 if e["success"] else 0
                attempts_fail += 0 if e["success"] else 1
                time_sum += e["time"]
                cur.execute("""
                    UPDATE lockpick_stats
                    SET attempts_total=%s, attempts_success=%s, attempts_fail=%s, time_sum=%s
                    WHERE nick=%s AND lock_type=%s
                """, (attempts_total, attempts_success, attempts_fail, time_sum, e["nick"], e["lock_type"]))
            else:
                attempts_total = 1
                attempts_success = 1 if e["success"] else 0
                attempts_fail = 0 if e["success"] else 1
                time_sum = e["time"]
                cur.execute("""
                    INSERT INTO lockpick_stats (nick, lock_type, attempts_total, attempts_success, attempts_fail, time_sum)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (e["nick"], e["lock_type"], attempts_total, attempts_success, attempts_fail, time_sum))
    conn.commit()

def generate_markdown_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nick, lock_type, attempts_total, attempts_success, attempts_fail, 
                   CASE WHEN attempts_total>0 THEN ROUND(100.0*attempts_success/attempts_total,2) ELSE 0 END AS efficiency,
                   CASE WHEN attempts_total>0 THEN ROUND(time_sum/attempts_total,2) ELSE 0 END AS avg_time
            FROM lockpick_stats
            ORDER BY nick, lock_type
        """)
        rows = cur.fetchall()

    # Przygotowanie szerokości kolumn
    headers = ["Nick", "Zamek", "Próby", "Udane", "Nieudane", "Skuteczność (%)", "Śr. czas"]
    cols = list(zip(*([headers] + rows)))
    col_widths = [max(len(str(x)) for x in col) for col in cols]

    # Generowanie tabeli Markdown z wyśrodkowaniem
    def center_text(text, width):
        text = str(text)
        padding = width - len(text)
        left_pad = padding // 2
        right_pad = padding - left_pad
        return " " * left_pad + text + " " * right_pad

    header_line = "| " + " | ".join(center_text(h, w) for h, w in zip(headers, col_widths)) + " |"
    sep_line = "|-" + "-|-".join("-" * w for w in col_widths) + "-|"
    rows_lines = []
    for row in rows:
        rows_lines.append("| " + " | ".join(center_text(c, w) for c, w in zip(row, col_widths)) + " |")

    table = "\n".join([header_line, sep_line] + rows_lines)
    return table

def print_table_console(table_text):
    print("[TABLE]")
    print(table_text)
    print("[/TABLE]")

def send_table_webhook(table_text):
    data = {"content": f"```\n{table_text}\n```"}
    resp = requests.post(WEBHOOK_URL, json=data)
    if resp.status_code == 204:
        print("[INFO] Wysłano tabelę na webhook.")
    else:
        print(f"[ERROR] Błąd wysyłki webhook: {resp.status_code} {resp.text}")

def main():
    print("[DEBUG] Start programu")
    try:
        conn = connect_db()
        create_tables(conn)

        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)

        all_files = list_files_ftp(ftp)
        log_files = [f for f in all_files if f.startswith("gameplay_") and f.endswith(".log")]

        print(f"[DEBUG] Znaleziono plików: {len(log_files)}")

        total_new_entries = 0
        for filename in log_files:
            if is_log_processed(conn, filename):
                print(f"[INFO] Pomijam już przetworzony plik: {filename}")
                continue

            content = download_log_file(ftp, filename)
            print(f"[INFO] Przetwarzam plik: {filename}")

            entries = parse_log_content(content)
            print(f"[DEBUG] {filename} -> {len(entries)} wpisów")

            if entries:
                update_stats(conn, entries)
                mark_log_processed(conn, filename)
                total_new_entries += len(entries)

        if total_new_entries == 0:
            print("[INFO] Brak nowych wpisów do przetworzenia.")
        else:
            table_text = generate_markdown_table(conn)
            print_table_console(table_text)
            send_table_webhook(table_text)

        ftp.quit()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Wystąpił błąd: {e}")

if __name__ == "__main__":
    main()
