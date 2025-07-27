import os
import re
import psycopg2
import requests
import threading
import time
from flask import Flask
from datetime import datetime
from ftplib import FTP
from io import BytesIO

# === KONFIGURACJE ===
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
    "sslmode": "require",
}

app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            castle TEXT,
            result TEXT,
            time REAL,
            UNIQUE(nick, castle, result, time)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DEBUG] Baza danych zainicjalizowana")

def fetch_all_log_files():
    print("[DEBUG] Nawiązywanie połączenia FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)

    lines = []
    ftp.dir(lines.append)

    filenames = [line.split()[-1] for line in lines if line.lower().endswith(".log") and line.startswith("gameplay_")]
    logs = []
    for filename in filenames:
        try:
            print(f"[DEBUG] Pobieranie pliku: {filename}")
            file_data = BytesIO()
            ftp.retrbinary(f"RETR {filename}", file_data.write)
            logs.append(file_data.getvalue().decode("utf-16-le"))
        except Exception as e:
            print(f"[ERROR] Nie udało się pobrać pliku {filename}: {e}")
    ftp.quit()
    print(f"[DEBUG] Pobieranie zakończone. Liczba logów: {len(logs)}")
    return logs

def parse_log_entries(log_text):
    pattern = re.compile(
        r"(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): \[Lockpicking\] ([^\s]+) tried to open lock at castle ([^\s]+): (SUCCESS|FAILED) in ([\d\.]+) seconds"
    )
    matches = pattern.findall(log_text)
    return [(nick, castle, result, float(time)) for _, nick, castle, result, time in matches]

def save_to_db(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_count = 0
    for nick, castle, result, time_ in entries:
        try:
            cur.execute("""
                INSERT INTO lockpicking (nick, castle, result, time)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (nick, castle, result, time_))
            if cur.rowcount:
                new_count += 1
        except Exception as e:
            print(f"[ERROR] Błąd przy zapisie do bazy: {e}")
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Zapisano {new_count} nowych wpisów do bazy.")
    return new_count

def generate_stats():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, castle,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE result = 'SUCCESS') AS success,
               COUNT(*) FILTER (WHERE result = 'FAILED') AS failed,
               ROUND(100.0 * COUNT(*) FILTER (WHERE result = 'SUCCESS') / NULLIF(COUNT(*), 0), 1) AS efficiency,
               ROUND(AVG(time), 2) AS avg_time
        FROM lockpicking
        GROUP BY nick, castle
        ORDER BY efficiency DESC NULLS LAST;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]

    table = "```"
    table += " | ".join(header.center(col_widths[i]) for i, header in enumerate(headers)) + "\n"
    table += "-+-".join('-' * w for w in col_widths) + "\n"
    for row in rows:
        table += " | ".join(str(col).center(col_widths[i]) for i, col in enumerate(row)) + "\n"
    table += "```"
    return table

def send_webhook(table):
    payload = {
        "content": table
    }
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        print(f"[DEBUG] Webhook response: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Webhook error: {e}")

def process_logs():
    logs = fetch_all_log_files()
    all_entries = []
    for log_text in logs:
        entries = parse_log_entries(log_text)
        all_entries.extend(entries)
    print(f"[DEBUG] Łącznie wpisów: {len(all_entries)}")
    new_count = save_to_db(all_entries)
    return new_count

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        print("[DEBUG] Sprawdzanie nowych wpisów...")
        new_entries = process_logs()
        if new_entries > 0:
            print("[DEBUG] Nowe dane – generuję i wysyłam statystyki")
            table = generate_stats()
            send_webhook(table)
        else:
            print("[DEBUG] Brak nowych wpisów – nic nie wysyłam")
        time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=main_loop).start()
    app.run(host='0.0.0.0', port=3000)
