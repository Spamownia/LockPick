import os
import re
import threading
import time
import hashlib
import psycopg2
from ftplib import FTP
from datetime import datetime
from flask import Flask
import requests

# === [ CONFIG ] ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

LOG_PATTERN = re.compile(
    r"\[(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] \[(?P<nick>.*?)\] Lockpicking (?P<castle>.*?) (?P<result>SUCCESS|FAILED) in (?P<duration>\d+\.\d+) sec"
)

# === [ INIT APP ] ===
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            castle TEXT,
            result TEXT,
            duration FLOAT,
            timestamp TIMESTAMP,
            raw_line_hash TEXT UNIQUE
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[INFO] Inicjalizacja bazy danych...")

def get_ftp_connection():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print(f"[DEBUG] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")
    return ftp

def fetch_log_files():
    ftp = get_ftp_connection()
    ftp.cwd(FTP_LOG_DIR)

    files = []

    def parse_line(line):
        parts = line.split()
        if len(parts) < 9:
            return
        name = parts[-1]
        if name.startswith("gameplay_") and name.endswith(".log"):
            files.append(name)

    ftp.retrlines('LIST', parse_line)

    log_entries = []

    for filename in files:
        print(f"[INFO] Przetwarzanie pliku: {filename}")
        try:
            with open(filename, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)

            with open(filename, "r", encoding="utf-16-le") as f:
                for line in f:
                    match = LOG_PATTERN.search(line)
                    if match:
                        data = match.groupdict()
                        data["timestamp"] = datetime.strptime(data["time"], "%Y-%m-%d %H:%M:%S")
                        data["duration"] = float(data["duration"])
                        data["result"] = data["result"].upper()
                        data["raw_line_hash"] = hashlib.sha256(line.strip().encode()).hexdigest()
                        log_entries.append(data)
        except Exception as e:
            print(f"[ERROR] Błąd podczas przetwarzania {filename}: {e}")
        finally:
            os.remove(filename)

    ftp.quit()
    print(f"[DEBUG] Liczba wpisów z logów: {len(log_entries)}")
    return log_entries

def insert_logs(logs):
    if not logs:
        return 0
    conn = connect_db()
    cur = conn.cursor()
    inserted = 0
    for entry in logs:
        try:
            cur.execute("""
                INSERT INTO lockpick_logs (nick, castle, result, duration, timestamp, raw_line_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (raw_line_hash) DO NOTHING
            """, (
                entry["nick"],
                entry["castle"],
                entry["result"],
                entry["duration"],
                entry["timestamp"],
                entry["raw_line_hash"]
            ))
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            print(f"[ERROR] Wstawianie danych: {e}")
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Nowe wpisy dodane: {inserted}")
    return inserted

def format_stats_table(stats):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [len(h) for h in headers]

    for row in stats:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    header_line = " | ".join(h.center(col_widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
    rows = [
        " | ".join(str(val).center(col_widths[i]) for i, val in enumerate(row))
        for row in stats
    ]
    return "\n".join([header_line, separator] + rows)

def generate_and_send_stats():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, castle,
            COUNT(*) AS total,
            SUM(CASE WHEN result = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN result = 'FAILED' THEN 1 ELSE 0 END) AS fail_count,
            ROUND(SUM(CASE WHEN result = 'SUCCESS' THEN 1 ELSE 0 END)::decimal * 100 / COUNT(*), 2) AS efficiency,
            ROUND(AVG(duration), 2) AS avg_time
        FROM lockpick_logs
        GROUP BY nick, castle
        ORDER BY efficiency DESC, avg_time ASC
    """)
    stats = cur.fetchall()
    cur.close()
    conn.close()

    if not stats:
        print("[INFO] Brak statystyk do wysłania.")
        return

    table = format_stats_table(stats)
    payload = {"content": f"```\n{table}\n```"}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        print(f"[INFO] Wysłano webhook: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Wysyłanie webhooka: {e}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    new_entries = fetch_log_files()
    added = insert_logs(new_entries)
    if added > 0:
        generate_and_send_stats()
    else:
        print("[INFO] Brak nowych danych do analizy.")

# === [ LAUNCH THREAD ] ===
threading.Thread(target=main_loop).start()

# === [ FLASK APP START ] ===
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
