import os
import re
import time
import psycopg2
import threading
import requests
from io import BytesIO
from datetime import datetime
from flask import Flask
from ftplib import FTP

# ---- CONFIG ----
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

# ---- APP ----
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# ---- GLOBAL ----
initialized = False

# ---- DATABASE ----
def init_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS lockpick_stats (
                    nick TEXT,
                    castle TEXT,
                    result TEXT,
                    duration FLOAT,
                    timestamp TIMESTAMP,
                    source_file TEXT
                );
            ''')
            conn.commit()

def insert_entry(nick, castle, result, duration, timestamp, source_file):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT 1 FROM lockpick_stats
                WHERE nick=%s AND castle=%s AND result=%s AND duration=%s AND timestamp=%s AND source_file=%s
            ''', (nick, castle, result, duration, timestamp, source_file))
            if not cur.fetchone():
                cur.execute('''
                    INSERT INTO lockpick_stats (nick, castle, result, duration, timestamp, source_file)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (nick, castle, result, duration, timestamp, source_file))
                conn.commit()

# ---- FTP & PARSER ----
def list_ftp_files(ftp):
    files = []

    def parse_line(line):
        parts = line.split()
        if len(parts) >= 9:
            filename = parts[-1]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                files.append(filename)

    ftp.retrlines("LIST " + FTP_LOG_DIR, parse_line)
    return files

def download_and_parse_log(ftp, filename):
    print(f"[INFO] Przetwarzanie: {filename}")
    full_path = FTP_LOG_DIR + filename
    buffer = BytesIO()
    ftp.retrbinary(f"RETR {full_path}", buffer.write)
    buffer.seek(0)
    text = buffer.read().decode('utf-16-le', errors='ignore')
    entries = []

    for match in re.finditer(
        r'(?P<timestamp>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): '
        r'(?P<nick>.*?) tried to pick a lock on (?P<castle>.*?)\'s (?P<result>success|fail) in (?P<duration>[0-9.]+) seconds',
        text):
        ts = datetime.strptime(match.group('timestamp'), "%Y.%m.%d-%H.%M.%S")
        entries.append({
            "timestamp": ts,
            "nick": match.group("nick"),
            "castle": match.group("castle"),
            "result": match.group("result"),
            "duration": float(match.group("duration")),
            "source_file": filename
        })

    print(f"[INFO] Wpisów znalezionych: {len(entries)}")
    return entries

def fetch_log_files():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print("[INFO] Połączono z FTP")
    files = list_ftp_files(ftp)
    print(f"[INFO] Znalezione pliki logów: {len(files)}")

    all_entries = []
    for fname in files:
        if fname.startswith("gameplay_") and fname.endswith(".log"):
            entries = download_and_parse_log(ftp, fname)
            all_entries.extend(entries)
    ftp.quit()
    return all_entries

# ---- WEBHOOK ----
def send_summary_to_webhook():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT nick, castle,
                       COUNT(*) AS total,
                       SUM(CASE WHEN result='success' THEN 1 ELSE 0 END) AS success_count,
                       SUM(CASE WHEN result='fail' THEN 1 ELSE 0 END) AS fail_count,
                       ROUND(SUM(CASE WHEN result='success' THEN 1 ELSE 0 END)::float * 100 / COUNT(*), 1) AS accuracy,
                       ROUND(AVG(duration), 2) AS avg_time
                FROM lockpick_stats
                GROUP BY nick, castle
                ORDER BY accuracy DESC NULLS LAST;
            ''')
            rows = cur.fetchall()

    if not rows:
        print("[INFO] Brak danych do wysyłki.")
        return

    lines = ["```", f"{'Nick':^15} {'Zamek':^15} {'Ilość prób':^12} {'Udane':^8} {'Nieudane':^10} {'Skuteczność':^13} {'Śr. czas':^10}"]
    for row in rows:
        lines.append(f"{row[0]:^15} {row[1]:^15} {row[2]:^12} {row[3]:^8} {row[4]:^10} {row[5]:^12}% {row[6]:^10.2f}")
    lines.append("```")

    requests.post(WEBHOOK_URL, json={"content": "\n".join(lines)})
    print("[INFO] Statystyki wysłane.")

# ---- MAIN LOOP ----
def main_loop():
    global initialized
    print("[DEBUG] Start main_loop")

    if not initialized:
        print("[INFO] Inicjalizacja bazy danych...")
        init_db()
        entries = fetch_log_files()
        print(f"[INFO] Łącznie wpisów: {len(entries)}")
        for e in entries:
            insert_entry(**e)
        send_summary_to_webhook()
        initialized = True

    while True:
        print("[DEBUG] Oczekiwanie na nowe wpisy...")
        time.sleep(60)

# ---- START ----
if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
