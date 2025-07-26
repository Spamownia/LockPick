import ftplib
import io
import re
import psycopg2
import requests
from collections import defaultdict

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# Konfiguracja DB PostgreSQL Neon
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Regex do parsowania wpisów lockpick
LOG_PATTERN = re.compile(
    r"""
    ^\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\s
    \[LogMinigame\]\s\[LockpickingMinigame_C\]\sUser:\s
    (?P<nick>.+?)\s\(\d+,.*?\)\.\s
    Success:\s(?P<result>Yes|No)\.\s
    Elapsed\stime:\s(?P<time>[\d\.]+)\.\s
    Failed\sattempts:\s\d+\.\s
    Target\sobject:.*?\.\s
    Lock\stype:\s(?P<castle>\w+)\.
    """,
    re.VERBOSE | re.MULTILINE
)

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)
    return ftp

def fetch_logs():
    ftp = connect_ftp()
    files = ftp.nlst()
    log_files = [f for f in files if f.startswith("gameplay_")]
    logs = []
    for filename in log_files:
        bio = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        bio.seek(0)
        content = bio.read().decode("utf-16le")
        logs.append(content)
    ftp.quit()
    return logs

def parse_logs(logs):
    entries = []
    for log_text in logs:
        for match in LOG_PATTERN.finditer(log_text):
            time_str = match.group("time").rstrip(".")
            try:
                time = float(time_str)
            except ValueError:
                # Jeśli nadal błąd, pomiń wpis
                continue
            entries.append({
                "nick": match.group("nick"),
                "castle": match.group("castle"),
                "result": match.group("result"),
                "time": time
            })
    return entries

def connect_db():
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        sslmode=DB_CONFIG["sslmode"]
    )
    return conn

def create_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            castle TEXT,
            result TEXT,
            time REAL
        )
    """)

def entry_exists(cur, entry):
    cur.execute("""
        SELECT 1 FROM lockpick_stats WHERE
        nick = %s AND castle = %s AND result = %s AND time = %s
    """, (entry["nick"], entry["castle"], entry["result"], entry["time"]))
    return cur.fetchone() is not None

def save_entries(cur, conn, entries):
    new_count = 0
    for entry in entries:
        if not entry_exists(cur, entry):
            cur.execute("""
                INSERT INTO lockpick_stats (nick, castle, result, time)
                VALUES (%s, %s, %s, %s)
            """, (entry["nick"], entry["castle"], entry["result"], entry["time"]))
            new_count += 1
    conn.commit()
    return new_count

def aggregate_stats(cur):
    cur.execute("""
        SELECT nick, castle,
               COUNT(*) AS total,
               SUM(CASE WHEN result='Yes' THEN 1 ELSE 0 END) AS success,
               SUM(CASE WHEN result='No' THEN 1 ELSE 0 END) AS fail,
               AVG(time) AS avg_time
        FROM lockpick_stats
        GROUP BY nick, castle
        ORDER BY nick, castle
    """)
    return cur.fetchall()

def create_table_text(data):
    headers = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    rows = []

    for row in data:
        nick, castle, total, success, fail, avg_time = row
        efficiency = f"{(success / total * 100):.2f}%" if total > 0 else "0%"
        avg_time_str = f"{avg_time:.2f}" if avg_time is not None else "-"
        rows.append([nick, castle, str(total), str(success), str(fail), efficiency, avg_time_str])

    # Oblicz szerokości kolumn (max długość w każdej kolumnie)
    col_widths = [max(len(row[i]) for row in [headers] + rows) for i in range(len(headers))]

    # Funkcja do formatowania pojedynczej komórki wyśrodkowanie tekstu
    def center_text(text, width):
        return text.center(width)

    # Buduj nagłówek tabeli
    header_line = "| " + " | ".join(center_text(headers[i], col_widths[i]) for i in range(len(headers))) + " |"
    separator_line = "|" + "|".join("-" * (col_widths[i] + 2) for i in range(len(headers))) + "|"

    # Buduj wiersze danych
    data_lines = []
    for row in rows:
        line = "| " + " | ".join(center_text(row[i], col_widths[i]) for i in range(len(row))) + " |"
        data_lines.append(line)

    table_text = "\n".join([header_line, separator_line] + data_lines)
    return table_text

def send_webhook(table_text):
    data = {"content": f"```{table_text}```"}
    requests.post(WEBHOOK_URL, json=data)

def main():
    print("[INFO] Pobieranie logów...")
    logs = fetch_logs()

    print("[INFO] Parsowanie danych...")
    parsed_entries = parse_logs(logs)
    print(f"[DEBUG] Sparsowane wpisy lockpick: {len(parsed_entries)}")

    conn = connect_db()
    cur = conn.cursor()
    print("[INFO] Inicjalizacja bazy...")
    create_table(cur)

    print("[INFO] Zapisywanie do bazy...")
    new_entries_count = save_entries(cur, conn, parsed_entries)
    print(f"[DEBUG] Nowe wpisy: {new_entries_count}")

    if new_entries_count > 0:
        stats = aggregate_stats(cur)
        table_text = create_table_text(stats)
        send_webhook(table_text)
        print("[INFO] Wysłano tabelę na webhook.")
    else:
        print("[INFO] Brak nowych danych – webhook nie wysłany.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
