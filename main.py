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

# === Konfiguracja ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOGS_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# === Połączenie z bazą ===
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            zamek TEXT,
            success BOOLEAN,
            czas FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[INFO] Inicjalizacja bazy danych...")

# === Parsowanie logów ===
def parse_log(content):
    entries = []
    lines = content.splitlines()
    for line in lines:
        match = re.search(r'Lockpicking: (.+?) tried to pick (.+?) lock and (succeeded|failed) in ([0-9.]+)s', line)
        if match:
            nick, zamek, status, czas = match.groups()
            success = status == "succeeded"
            entries.append((nick, zamek, success, float(czas)))
    return entries

# === Zapis do bazy ===
def insert_entries(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for entry in entries:
        cur.execute("""
            INSERT INTO lockpick_stats (nick, zamek, success, czas)
            VALUES (%s, %s, %s, %s)
        """, entry)
    conn.commit()
    cur.close()
    conn.close()

# === Tworzenie tabeli wyników ===
def generate_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, zamek,
            COUNT(*) AS total,
            SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS fail_count,
            ROUND(100.0 * SUM(CASE WHEN success THEN 1 ELSE 0 END) / COUNT(*), 2) AS skutecznosc,
            ROUND(AVG(czas), 2) AS sredni_czas
        FROM lockpick_stats
        GROUP BY nick, zamek
        ORDER BY skutecznosc DESC, total DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = "```" + "\n"
    col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]
    row_format = " | ".join(f"{{:^{w}}}" for w in col_widths)
    table += row_format.format(*headers) + "\n"
    table += "-+-".join("-" * w for w in col_widths) + "\n"
    for row in rows:
        table += row_format.format(*row) + "\n"
    table += "```"
    return table if rows else None

# === Wysyłka na webhook ===
def send_webhook(message):
    requests.post(WEBHOOK_URL, json={"content": message})

# === Pobieranie logów ===
already_processed = set()

def fetch_log_files():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOGS_PATH)
    files = ftp.nlst()
    gameplay_logs = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

    all_new_entries = []
    for filename in gameplay_logs:
        if filename in already_processed:
            continue
        print(f"[DEBUG] Przetwarzanie pliku: {filename}")
        with io.BytesIO() as bio:
            ftp.retrbinary(f"RETR {filename}", bio.write)
            bio.seek(0)
            content = codecs.decode(bio.read(), "utf-16-le")
            entries = parse_log(content)
            if entries:
                all_new_entries.extend(entries)
                insert_entries(entries)
                print(f"[INFO] Dodano {len(entries)} wpisów z {filename}")
            else:
                print(f"[INFO] Brak pasujących wpisów w {filename}")
        already_processed.add(filename)
    ftp.quit()
    return all_new_entries

# === Główna pętla ===
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        new_entries = fetch_log_files()
        if new_entries:
            print(f"[INFO] Wykryto {len(new_entries)} nowych wpisów.")
            tabela = generate_table()
            if tabela:
                send_webhook(tabela)
        else:
            print("[INFO] Brak nowych wpisów.")
        time.sleep(60)

# === Uruchomienie aplikacji ===
if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
