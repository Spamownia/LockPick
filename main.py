import os
import re
import ftplib
import psycopg2
import requests
from collections import defaultdict
from io import BytesIO

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# Konfiguracja DB
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

def list_log_files(ftp):
    files = []

    def parse_line(line):
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            name = parts[8]
            if name.startswith("gameplay_") and name.endswith(".log"):
                files.append(name)

    ftp.dir(LOG_DIR, parse_line)
    return files

def connect_to_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def download_log_file(ftp, filename):
    path = f"{LOG_DIR}/{filename}"
    buffer = BytesIO()
    ftp.retrbinary(f"RETR {path}", buffer.write)
    buffer.seek(0)
    return buffer.read().decode('utf-16-le', errors='ignore')

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_stats (
            nick TEXT,
            lock_type TEXT,
            total_attempts INT,
            success_count INT,
            failure_count INT,
            total_time FLOAT,
            PRIMARY KEY (nick, lock_type)
        )
    """)
    conn.commit()
    return conn

def parse_log_content(content):
    pattern = re.compile(
        r'User: (?P<nick>.+?) \(\d+, \d+\)\. '
        r'Success: (?P<success>Yes|No)\. '
        r'Elapsed time: (?P<time>[\d.]+)\. '
        r'Failed attempts: (?P<fail>\d+)\. .*?'
        r'Lock type: (?P<lock_type>\w+)\.'
    )

    results = []
    for match in pattern.finditer(content):
        nick = match.group("nick").strip()
        lock_type = match.group("lock_type").strip()
        time = float(match.group("time"))
        success = match.group("success") == "Yes"
        results.append((nick, lock_type, time, success))
    return results

def update_stats_in_db(conn, parsed_data):
    cur = conn.cursor()
    stats = defaultdict(lambda: {"total": 0, "success": 0, "failure": 0, "time": 0.0})

    for nick, lock_type, time, success in parsed_data:
        key = (nick, lock_type)
        stats[key]["total"] += 1
        stats[key]["success" if success else "failure"] += 1
        stats[key]["time"] += time

    for (nick, lock_type), data in stats.items():
        cur.execute("""
            INSERT INTO lockpicking_stats (nick, lock_type, total_attempts, success_count, failure_count, total_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (nick, lock_type) DO UPDATE
            SET total_attempts = lockpicking_stats.total_attempts + EXCLUDED.total_attempts,
                success_count = lockpicking_stats.success_count + EXCLUDED.success_count,
                failure_count = lockpicking_stats.failure_count + EXCLUDED.failure_count,
                total_time = lockpicking_stats.total_time + EXCLUDED.total_time
        """, (nick, lock_type, data["total"], data["success"], data["failure"], data["time"]))
    conn.commit()

def generate_table(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM lockpicking_stats")
    rows = cur.fetchall()

    if not rows:
        return "Brak danych do wyświetlenia."

    headers = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [len(h) for h in headers]
    table_rows = []

    for nick, lock_type, total, success, failure, time in rows:
        efficiency = f"{(success / total * 100):.1f}%"
        avg_time = f"{(time / total):.2f}s"
        row = [nick, lock_type, str(total), str(success), str(failure), efficiency, avg_time]
        table_rows.append(row)
        for i, item in enumerate(row):
            col_widths[i] = max(col_widths[i], len(item))

    def format_row(row):
        return " | ".join(item.center(col_widths[i]) for i, item in enumerate(row))

    header = format_row(headers)
    separator = "-+-".join("-" * w for w in col_widths)
    rows_formatted = [format_row(r) for r in table_rows]

    return f"```\n{header}\n{separator}\n" + "\n".join(rows_formatted) + "\n```"

def send_to_webhook(content):
    requests.post(WEBHOOK_URL, json={"content": content})

def main():
    print("[DEBUG] Start main")
    conn = init_db()
    ftp = connect_to_ftp()
    files = list_log_files(ftp)
    print(f"[DEBUG] Znalezione pliki: {files}")

    for filename in files:
        print(f"[DEBUG] Przetwarzanie: {filename}")
        content = download_log_file(ftp, filename)
        parsed = parse_log_content(content)
        print(f"[DEBUG] Rozpoznane wpisy: {len(parsed)}")
        update_stats_in_db(conn, parsed)

    summary = generate_table(conn)
    send_to_webhook(summary)
    print("[DEBUG] Zakończono.")

if __name__ == "__main__":
    main()
