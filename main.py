import os
import re
import io
import ftplib
import psycopg2
import requests
import datetime
from flask import Flask

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

# --- PARSER ---
LOG_PATTERN = re.compile(
    r"\[\s*LogMinigame\s*]\s*\[LockpickingMinigame_C]\s*User:\s*(?P<nick>.*?)\s*\(\d+,\s*\d+\).*?"
    r"Success:\s*(?P<success>Yes|No).*?"
    r"Elapsed time:\s*(?P<time>[\d.]+).*?"
    r"Failed attempts:\s*(?P<fail>\d+).*?"
    r"Lock type:\s*(?P<lock>[\w]+)"
)

def fetch_logs():
    print("[INFO] Pobieranie logów...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)
    print("[INFO] Połączono z FTP.")

    files = []

    def parse_line(line):
        parts = line.split()
        filename = parts[-1]
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            files.append(filename)

    ftp.retrlines("LIST", callback=parse_line)
    print(f"[DEBUG] Liczba logów: {len(files)}")

    logs = []
    for filename in files:
        with io.BytesIO() as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
            f.seek(0)
            content = f.read().decode("utf-16-le", errors="ignore")
            logs.append(content)

    ftp.quit()
    return logs

def parse_logs(log_texts):
    print("[INFO] Parsowanie danych...")
    entries = []
    for text in log_texts:
        for match in LOG_PATTERN.finditer(text):
            nick = match.group("nick").strip()
            lock = match.group("lock").strip()
            success = match.group("success") == "Yes"
            time = float(match.group("time"))
            fail = int(match.group("fail"))
            total = fail + 1
            entries.append((nick, lock, success, time, total))
    print(f"[DEBUG] Sparsowane wpisy lockpick: {len(entries)}")
    return entries

def save_to_db(entries):
    print("[INFO] Inicjalizacja bazy...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            lock TEXT,
            success BOOLEAN,
            time FLOAT,
            total_attempts INTEGER,
            timestamp TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.commit()

    new_rows = 0
    for nick, lock, success, time, total in entries:
        cur.execute("""
            SELECT COUNT(*) FROM lockpick_stats
            WHERE nick = %s AND lock = %s AND success = %s AND time = %s AND total_attempts = %s
        """, (nick, lock, success, time, total))
        exists = cur.fetchone()[0] > 0
        if not exists:
            cur.execute("""
                INSERT INTO lockpick_stats (nick, lock, success, time, total_attempts)
                VALUES (%s, %s, %s, %s, %s)
            """, (nick, lock, success, time, total))
            new_rows += 1

    conn.commit()
    cur.close()
    conn.close()
    print("[INFO] Zapisywanie do bazy...")
    print(f"[DEBUG] Nowe wpisy: {new_rows}")
    return new_rows > 0

def send_stats():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, lock,
            COUNT(*) as all_count,
            SUM(CASE WHEN success THEN 1 ELSE 0 END) as succ,
            SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as fail,
            ROUND(100.0 * SUM(CASE WHEN success THEN 1 ELSE 0 END) / COUNT(*), 2) as rate,
            ROUND(AVG(time), 2) as avg_time
        FROM lockpick_stats
        GROUP BY nick, lock
        ORDER BY succ DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    headers = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]

    def format_row(row):
        return " | ".join(str(col).center(col_widths[i]) for i, col in enumerate(row))

    table = format_row(headers) + "\n" + "-+-".join("-" * w for w in col_widths)
    for row in rows:
        table += "\n" + format_row(row)

    requests.post(WEBHOOK_URL, json={"content": f"```\n{table}\n```"})
    print("[INFO] Wysłano webhook.")

def main():
    logs = fetch_logs()
    parsed = parse_logs(logs)
    if save_to_db(parsed):
        send_stats()
    else:
        print("[INFO] Brak nowych danych – webhook nie wysłany.")

if __name__ == "__main__":
    main()
    app.run(host="0.0.0.0", port=3000)
