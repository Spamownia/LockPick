import os
import re
import psycopg2
import requests
import threading
from flask import Flask
from datetime import datetime
from ftplib import FTP_TLS
from io import BytesIO

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require",
}

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)

def init_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_stats (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    zamek TEXT,
                    wynik TEXT,
                    czas INTEGER,
                    unikalny_id TEXT UNIQUE
                );
            """)
        conn.commit()
    print("[DEBUG] Baza danych zainicjalizowana")

def extract_data_from_log(content):
    entries = []
    lines = content.splitlines()
    for line in lines:
        match = re.search(r'LockPickingComponent.*?nick: (\S+), zamek: (\S+), wynik: (\S+), czas: (\d+)', line)
        if match:
            nick, zamek, wynik, czas = match.groups()
            unikalny_id = f"{nick}_{zamek}_{wynik}_{czas}"
            entries.append((nick, zamek, wynik, int(czas), unikalny_id))
    return entries

def fetch_all_log_files():
    print("[DEBUG] Nawiązywanie połączenia FTP...")
    ftps = FTP_TLS()
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.login(FTP_USER, FTP_PASS)
    ftps.prot_p()
    ftps.cwd(LOG_DIR)

    filenames = []
    ftps.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
    log_files = [name for name in filenames if name.startswith("gameplay_") and name.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików logów.")
    logs_data = []

    for log_file in log_files:
        buffer = BytesIO()
        ftps.retrbinary(f"RETR {log_file}", buffer.write)
        buffer.seek(0)
        content = buffer.read().decode("utf-16-le", errors="ignore")
        logs_data.append(content)
        print(f"[DEBUG] Pobrano {log_file}, {len(content.splitlines())} linii")

    ftps.quit()
    return logs_data

def insert_new_entries(entries):
    new_count = 0
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for entry in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpick_stats (nick, zamek, wynik, czas, unikalny_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (unikalny_id) DO NOTHING;
                    """, entry)
                    if cur.rowcount > 0:
                        new_count += 1
                except Exception as e:
                    print(f"[ERROR] Błąd przy wstawianiu wpisu {entry}: {e}")
            conn.commit()
    print(f"[DEBUG] Wstawiono {new_count} nowych wpisów do bazy")
    return new_count

def fetch_stats():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, zamek,
                       COUNT(*) AS proby,
                       COUNT(*) FILTER (WHERE wynik = 'sukces') AS udane,
                       COUNT(*) FILTER (WHERE wynik = 'pudlo') AS nieudane,
                       ROUND(
                           100.0 * COUNT(*) FILTER (WHERE wynik = 'sukces') / NULLIF(COUNT(*), 0),
                           2
                       ) AS skutecznosc,
                       ROUND(AVG(czas), 2) AS sredni_czas
                FROM lockpick_stats
                GROUP BY nick, zamek
                ORDER BY skutecznosc DESC NULLS LAST, sredni_czas ASC;
            """)
            return cur.fetchall()

def format_table(data):
    headers = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = [headers] + [[str(cell) for cell in row] for row in data]
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*table)]
    lines = []
    for row in table:
        line = " | ".join(cell.center(width) for cell, width in zip(row, col_widths))
        lines.append(line)
    return "```\n" + "\n".join(lines) + "\n```"

def send_to_webhook(message):
    response = requests.post(WEBHOOK_URL, json={"content": message})
    if response.status_code == 204:
        print("[DEBUG] Wysłano dane na webhook")
    else:
        print(f"[ERROR] Webhook zwrócił kod {response.status_code}: {response.text}")

def process_logs():
    logs = fetch_all_log_files()
    all_entries = []
    for content in logs:
        entries = extract_data_from_log(content)
        all_entries.extend(entries)
    new_count = insert_new_entries(all_entries)
    return new_count

def main_loop():
    print("[DEBUG] Start main_loop")
    while True:
        new_entries = process_logs()
        if new_entries > 0:
            stats = fetch_stats()
            table = format_table(stats)
            send_to_webhook(table)
        else:
            print("[DEBUG] Brak nowych danych w logach")
        threading.Event().wait(60)

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    init_db()
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
