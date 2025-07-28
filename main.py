import re
import ftplib
import psycopg2
import requests
from tabulate import tabulate
from io import BytesIO

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# Konfiguracja bazy danych Neon
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Funkcje ---

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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_stats (
                nick TEXT NOT NULL,
                lock_type TEXT NOT NULL,
                total_attempts INTEGER NOT NULL,
                success_count INTEGER NOT NULL,
                failed_count INTEGER NOT NULL,
                avg_time FLOAT NOT NULL,
                PRIMARY KEY (nick, lock_type)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_logs (
                filename TEXT PRIMARY KEY
            );
        """)
    conn.commit()

def is_log_processed(conn, filename):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM processed_logs WHERE filename = %s;", (filename,))
        return cur.fetchone() is not None

def mark_log_processed(conn, filename):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO processed_logs (filename) VALUES (%s) ON CONFLICT DO NOTHING;", (filename,))
    conn.commit()

def update_stats(conn, entries):
    with conn.cursor() as cur:
        for e in entries:
            cur.execute("""
                SELECT total_attempts, success_count, failed_count, avg_time
                FROM lockpicking_stats
                WHERE nick = %s AND lock_type = %s;
            """, (e["nick"], e["lock_type"]))
            row = cur.fetchone()
            if row:
                total, success, failed, avg = row
                total_new = total + 1
                success_new = success + (1 if e["success"] else 0)
                failed_new = failed + (0 if e["success"] else 1)
                avg_new = ((avg * total) + e["elapsed_time"]) / total_new
                cur.execute("""
                    UPDATE lockpicking_stats
                    SET total_attempts = %s,
                        success_count = %s,
                        failed_count = %s,
                        avg_time = %s
                    WHERE nick = %s AND lock_type = %s;
                """, (total_new, success_new, failed_new, avg_new, e["nick"], e["lock_type"]))
            else:
                cur.execute("""
                    INSERT INTO lockpicking_stats (nick, lock_type, total_attempts, success_count, failed_count, avg_time)
                    VALUES (%s, %s, 1, %s, %s, %s);
                """, (e["nick"], e["lock_type"], 1 if e["success"] else 0, 0 if e["success"] else 1, e["elapsed_time"]))
    conn.commit()

def parse_log_content(content):
    pattern = re.compile(
        r"User:\s(?P<nick>.+?)\s\(.+?\)\.\sSuccess:\s(?P<success>Yes|No)\.\sElapsed time:\s(?P<elapsed_time>[\d\.]+)\."
        r"\sFailed attempts:\s(?P<failed_attempts>\d+)\.\sTarget object:.*?\.\sLock type:\s(?P<lock_type>.+?)\."
    )
    entries = []
    for line in content.splitlines():
        m = pattern.search(line)
        if m:
            entry = {
                "nick": m.group("nick").strip(),
                "success": m.group("success") == "Yes",
                "elapsed_time": float(m.group("elapsed_time")),
                "failed_attempts": int(m.group("failed_attempts")),
                "lock_type": m.group("lock_type").strip()
            }
            entries.append(entry)
    return entries

def download_log_file(ftp, filename):
    bio = BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    bio.seek(0)
    return bio.read().decode("utf-16le")

def generate_markdown_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nick, lock_type, total_attempts, success_count, failed_count,
            ROUND((success_count::decimal / total_attempts) * 100, 2) AS effectiveness,
            ROUND(avg_time, 2)
            FROM lockpicking_stats
            ORDER BY nick, lock_type;
        """)
        rows = cur.fetchall()
        if not rows:
            return None

        headers = ["Nick", "Lock", "Attempts", "Success", "Fail", "Effectiveness (%)", "Avg Time"]
        table_text = tabulate(rows, headers, tablefmt="github", numalign="center", stralign="center")
        return table_text

def print_table_console(table_text):
    if table_text:
        print("\n[INFO] Podsumowanie statystyk lockpicking:")
        print(table_text)
    else:
        print("[INFO] Brak danych do wyświetlenia.")

def send_table_webhook(table_text):
    if not table_text:
        print("[INFO] Brak danych do wysłania na webhook.")
        return
    payload = {"content": f"```\n{table_text}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[INFO] Tabela wysłana na webhook Discord.")
    else:
        print(f"[ERROR] Błąd wysyłki webhook: {response.status_code} {response.text}")

def main():
    print("[DEBUG] Start programu")

    try:
        conn = connect_db()
        create_tables(conn)

        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)

        all_files = ftp.nlst()
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
