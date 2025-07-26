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

# Konfiguracja FTP i webhook
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Konfiguracja bazy danych
DB_HOST = "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech"
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASS = "npg_dRU1YCtxbh6v"
DB_SSL = "require"
DB_PORT = 5432

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        sslmode=DB_SSL,
        port=DB_PORT
    )

def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_stats (
                    nick TEXT,
                    castle TEXT,
                    success BOOLEAN,
                    time FLOAT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

def parse_log(content):
    entries = []
    for line in content.splitlines():
        if "Lockpicking" in line and "attempt" in line:
            nick_match = re.search(r'CharacterName: (.*?)\)', line)
            castle_match = re.search(r'Castle: (.*?)\)', line)
            success = 'successful attempt' in line
            time_match = re.search(r'Time: ([\d.]+)', line)
            if nick_match and castle_match and time_match:
                nick = nick_match.group(1)
                castle = castle_match.group(1)
                time_val = float(time_match.group(1))
                entries.append((nick, castle, success, time_val))
    return entries

def list_files(ftp):
    files = []

    def parse_line(line):
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            name = parts[8]
            if name.startswith("gameplay_") and name.endswith(".log"):
                files.append(name)

    ftp.retrlines("LIST " + LOG_PATH, parse_line)
    return files

def fetch_log_files():
    print("[DEBUG] Pobieranie plików logów z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)

    files = list_files(ftp)
    new_entries = []

    for filename in files:
        full_path = os.path.join(LOG_PATH, filename)
        r = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {full_path}", r.write)
            r.seek(0)
            decoded = codecs.decode(r.read(), 'utf-16-le', errors='ignore')
            entries = parse_log(decoded)
            new_entries.extend(entries)
            print(f"[DEBUG] Przetworzono plik: {filename}, wpisów: {len(entries)}")
        except Exception as e:
            print(f"[ERROR] Błąd przy pobieraniu {filename}: {e}")

    ftp.quit()
    return new_entries

def save_to_db(entries):
    print(f"[INFO] Zapisywanie {len(entries)} wpisów do bazy danych...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            for entry in entries:
                cur.execute("""
                    INSERT INTO lockpick_stats (nick, castle, success, time)
                    VALUES (%s, %s, %s, %s)
                """, entry)
            conn.commit()

def aggregate_data():
    print("[DEBUG] Agregowanie danych z bazy...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, castle,
                    COUNT(*) AS total,
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS fail_count,
                    ROUND(SUM(CASE WHEN success THEN 1 ELSE 0 END)::float / COUNT(*) * 100, 2) AS effectiveness,
                    ROUND(AVG(time), 2) AS avg_time
                FROM lockpick_stats
                GROUP BY nick, castle
                ORDER BY effectiveness DESC;
            """)
            return cur.fetchall()

def format_table(data):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = [headers] + [[str(cell) for cell in row] for row in data]
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*table)]
    formatted = "```\n"
    for row in table:
        formatted += " | ".join(cell.center(w) for cell, w in zip(row, col_widths)) + "\n"
    formatted += "```"
    return formatted

def send_webhook(message):
    requests.post(WEBHOOK_URL, json={"content": message})

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        try:
            new_entries = fetch_log_files()
            if new_entries:
                save_to_db(new_entries)
                aggregated = aggregate_data()
                table = format_table(aggregated)
                send_webhook(table)
            else:
                print("[INFO] Brak nowych wpisów.")
        except Exception as e:
            print(f"[ERROR] Wystąpił błąd: {e}")
        time.sleep(60)

# Uruchomienie pętli w tle
threading.Thread(target=main_loop, daemon=True).start()

# Serwer sprawdzający alive
app.run(host="0.0.0.0", port=3000)
