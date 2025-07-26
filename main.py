import ftplib
import os
import re
import psycopg2
import requests
import threading
import time
import io
import codecs
from datetime import datetime
from flask import Flask

# Konfiguracja
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

processed_entries = set()

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            nick TEXT,
            castle TEXT,
            success BOOLEAN,
            time FLOAT,
            timestamp TIMESTAMP,
            UNIQUE(nick, castle, timestamp)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def parse_log_line(line):
    match = re.match(r".*?\] (.*?) tried to pick the lock of (.*?) and (succeeded|failed) in ([0-9.]+) seconds", line)
    if match:
        nick = match.group(1)
        castle = match.group(2)
        success = match.group(3) == "succeeded"
        time_taken = float(match.group(4))
        return nick, castle, success, time_taken
    return None

def fetch_log_files():
    print("[DEBUG] Pobieranie plików z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)

    files = []

    def collect_files(line):
        parts = line.split()
        filename = parts[-1]
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            files.append(filename)

    ftp.retrlines('LIST', collect_files)

    entries = []
    for file in files:
        print(f"[DEBUG] Przetwarzanie pliku: {file}")
        r = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {file}", r.write)
        except Exception as e:
            print(f"[ERROR] Nie można pobrać pliku {file}: {e}")
            continue
        content = r.getvalue().decode("utf-16le", errors="ignore")
        for line in content.splitlines():
            if "tried to pick the lock" in line:
                result = parse_log_line(line)
                if result:
                    nick, castle, success, time_taken = result
                    timestamp_match = re.match(r"(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})", line)
                    timestamp = datetime.strptime(timestamp_match.group(1), "%Y.%m.%d-%H.%M.%S") if timestamp_match else datetime.utcnow()
                    entry_id = (nick, castle, timestamp)
                    if entry_id not in processed_entries:
                        entries.append((nick, castle, success, time_taken, timestamp))
                        processed_entries.add(entry_id)
    ftp.quit()
    print(f"[DEBUG] Znaleziono {len(entries)} nowych wpisów")
    return entries

def insert_entries(entries):
    if not entries:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for e in entries:
        try:
            cur.execute("""
                INSERT INTO lockpick_logs (nick, castle, success, time, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, e)
        except Exception as ex:
            print(f"[ERROR] Błąd przy dodawaniu do DB: {ex}")
    conn.commit()
    cur.close()
    conn.close()

def generate_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, castle,
               COUNT(*) as attempts,
               COUNT(*) FILTER (WHERE success) as successes,
               COUNT(*) FILTER (WHERE NOT success) as failures,
               ROUND(100.0 * COUNT(*) FILTER (WHERE success) / COUNT(*), 2) as effectiveness,
               ROUND(AVG(time), 2) as avg_time
        FROM lockpick_logs
        GROUP BY nick, castle
        ORDER BY effectiveness DESC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [len(h) for h in headers]

    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    def format_row(row):
        return " | ".join(str(val).center(col_widths[i]) for i, val in enumerate(row))

    header = format_row(headers)
    separator = "-+-".join("-" * w for w in col_widths)
    body = "\n".join(format_row(r) for r in rows)
    return f"```\n{header}\n{separator}\n{body}\n```"

def send_to_webhook(table_text):
    print("[INFO] Wysyłanie tabeli do webhooka...")
    data = {"content": table_text}
    try:
        r = requests.post(WEBHOOK_URL, json=data)
        if r.status_code != 204:
            print(f"[ERROR] Webhook nie powiódł się: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"[ERROR] Wyjątek przy webhooku: {e}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        new_entries = fetch_log_files()
        if new_entries:
            print(f"[INFO] Dodano {len(new_entries)} nowych wpisów")
            insert_entries(new_entries)
            table = generate_table()
            send_to_webhook(table)
        else:
            print("[INFO] Brak nowych danych")
        time.sleep(60)

# Start wątku
threading.Thread(target=main_loop, daemon=True).start()

# Flask serwer
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000)
