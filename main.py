import os
import re
import time
import threading
import io
from ftplib import FTP
from datetime import datetime

import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from flask import Flask

# Konfiguracja Flask
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# Konfiguracja bazy danych
DB_CONFIG = {
    'host': "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    'dbname': "neondb",
    'user': "neondb_owner",
    'password': "npg_dRU1YCtxbh6v",
    'sslmode': "require"
}

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Inicjalizacja bazy
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicks (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            lock TEXT,
            result TEXT,
            elapsed FLOAT,
            timestamp TIMESTAMPTZ DEFAULT now()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# Parsowanie treści logu
def parse_log_content(content):
    entries = []
    pattern = re.compile(r'\[LogMinigame\].*?User: (.*?) .*?Lock type: (.*?) .*?Success: (Yes|No).*?Elapsed time: ([0-9.]+)', re.DOTALL)
    for match in pattern.finditer(content):
        nick, lock, result, elapsed = match.groups()
        entries.append({
            "nick": nick.strip(),
            "lock": lock.strip(),
            "result": result.strip(),
            "elapsed": float(elapsed)
        })
    return entries

# Zapis do bazy
def save_to_db(entries):
    if not entries:
        return 0
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_count = 0
    for entry in entries:
        cur.execute("""
            SELECT COUNT(*) FROM lockpicks
            WHERE nick=%s AND lock=%s AND result=%s AND elapsed=%s;
        """, (entry['nick'], entry['lock'], entry['result'], entry['elapsed']))
        exists = cur.fetchone()[0]
        if not exists:
            cur.execute("""
                INSERT INTO lockpicks (nick, lock, result, elapsed)
                VALUES (%s, %s, %s, %s);
            """, (entry['nick'], entry['lock'], entry['result'], entry['elapsed']))
            new_count += 1
    conn.commit()
    cur.close()
    conn.close()
    return new_count

# Generowanie tabeli wyników
def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT nick, lock, result, elapsed FROM lockpicks", conn)
    conn.close()

    if df.empty:
        return None

    df['success'] = df['result'] == "Yes"
    grouped = df.groupby(['nick', 'lock']).agg(
        attempts=('result', 'count'),
        success_count=('success', 'sum'),
        fail_count=('success', lambda x: (~x).sum()),
        success_rate=('success', lambda x: f"{(x.mean() * 100):.1f}%"),
        avg_time=('elapsed', lambda x: f"{x.mean():.2f}s")
    ).reset_index()

    # Wyśrodkowane kolumny
    table = tabulate(grouped, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="github", stralign="center", numalign="center")
    return table

# Wysyłka do Discorda
def send_to_discord(table):
    if table:
        payload = {"content": f"```\n{table}\n```"}
        requests.post(WEBHOOK_URL, json=payload)

# Pobranie logów z FTP
def fetch_log_files():
    entries = []
    try:
        with FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOG_PATH)
            filenames = []
            ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
            log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
            print(f"[DEBUG] Znaleziono plików: {len(log_files)}")
            for filename in log_files:
                bio = io.BytesIO()
                ftp.retrbinary(f"RETR {filename}", bio.write)
                bio.seek(0)
                text = io.TextIOWrapper(bio, encoding='utf-16-le').read()
                parsed = parse_log_content(text)
                print(f"[DEBUG] Plik: {filename} → wpisów: {len(parsed)}")
                entries.extend(parsed)
    except Exception as e:
        print(f"[ERROR] Błąd FTP: {e}")
    return entries

# Główna pętla
def main_loop():
    print("[DEBUG] Start programu")
    init_db()
    while True:
        entries = fetch_log_files()
        new_count = save_to_db(entries)
        if new_count:
            print(f"[INFO] Nowe wpisy zapisane: {new_count}")
            table = create_dataframe()
            send_to_discord(table)
        else:
            print("[INFO] Brak nowych wpisów.")
        time.sleep(60)

# Uruchomienie pętli w tle
threading.Thread(target=main_loop, daemon=True).start()

# Uruchomienie Flask
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000)
