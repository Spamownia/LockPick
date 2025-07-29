import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
from flask import Flask
from datetime import datetime
from io import BytesIO
from tabulate import tabulate

# --- Flask keep-alive ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- Konfiguracje ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- Funkcja pobierania logów z FTP ---
def fetch_logs_from_ftp():
    logs = {}
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_DIR)
            files = ftp.nlst()

            log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
            print(f"[DEBUG] Znaleziono {len(log_files)} plików logów.")

            for filename in log_files:
                with BytesIO() as bio:
                    ftp.retrbinary(f"RETR {filename}", bio.write)
                    content = bio.getvalue().decode("utf-16le")
                    logs[filename] = content
                    print(f"[DEBUG] Wczytano plik: {filename}, długość: {len(content)} znaków.")
    except Exception as e:
        print(f"[ERROR] Błąd FTP: {e}")
    return logs

# --- Parsowanie zawartości loga ---
def parse_log_content(log_content):
    entries = []
    pattern = r"User: (.*?) .*?Success: (Yes|No).*?Elapsed time: ([\d.]+)"
    matches = re.findall(pattern, log_content)

    for match in matches:
        user, success, elapsed_time = match
        entries.append({
            "Nick": user.strip(),
            "Success": success == "Yes",
            "ElapsedTime": float(elapsed_time),
            "LockType": "Unknown"  # Można rozszerzyć, jeśli typ zamka będzie dostępny
        })
    print(f"[DEBUG] Rozpoznano {len(entries)} wpisów w logu.")
    return entries

# --- Inicjalizacja bazy danych ---
def init_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicking (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    success BOOLEAN,
                    elapsed_time FLOAT,
                    lock_type TEXT,
                    UNIQUE(nick, success, elapsed_time, lock_type)
                )
            """)
        conn.commit()
    print("[DEBUG] Baza danych gotowa.")

# --- Zapis danych do bazy danych ---
def save_to_db(entries):
    new_entries = 0
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for e in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpicking (nick, success, elapsed_time, lock_type)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (e["Nick"], e["Success"], e["ElapsedTime"], e["LockType"]))
                    new_entries += cur.rowcount
                except Exception as err:
                    print(f"[ERROR] Błąd zapisu: {err}")
        conn.commit()
    print(f"[DEBUG] Zapisano {new_entries} nowych wpisów.")
    return new_entries > 0

# --- Tworzenie tabeli ---
def create_dataframe():
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql_query("SELECT * FROM lockpicking", conn)

    if df.empty:
        print("[DEBUG] Brak danych w bazie.")
        return "Brak danych."

    grouped = df.groupby(['nick', 'lock_type'])
    stats = grouped.agg(
        Total=('success', 'count'),
        Successes=('success', 'sum'),
        Failures=('success', lambda x: (~x).sum()),
        Effectiveness=('success', 'mean'),
        AvgTime=('elapsed_time', 'mean')
    ).reset_index()

    print(f"[DEBUG] Utworzono statystyki dla {len(stats)} graczy/zamków.")

    stats['Effectiveness'] = (stats['Effectiveness'] * 100).round(2).astype(str) + '%'
    stats['AvgTime'] = stats['AvgTime'].round(2).astype(str) + 's'

    stats.rename(columns={
        'nick': 'Nick',
        'lock_type': 'Zamek',
        'Total': 'Ilość prób',
        'Successes': 'Udane',
        'Failures': 'Nieudane',
        'Effectiveness': 'Skuteczność',
        'AvgTime': 'Średni czas'
    }, inplace=True)

    table = tabulate(stats, headers='keys', tablefmt='github', stralign='center', numalign='center')
    print("[DEBUG] Tabela gotowa:\n" + table)
    return table

# --- Wysyłanie do Discorda ---
def send_to_discord(table):
    import requests
    if not table or table == "Brak danych.":
        return
    data = {
        "content": f"```\n{table}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=data)
    print(f"[DEBUG] Wysłano do Discorda: status {response.status_code}")

# --- Pętla główna ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()

    while True:
        logs = fetch_logs_from_ftp()
        all_entries = []

        for filename, content in logs.items():
            parsed = parse_log_content(content)
            all_entries.extend(parsed)

        if not all_entries:
            print("[DEBUG] Brak nowych danych w logach.")
        else:
            has_new = save_to_db(all_entries)
            if has_new:
                table = create_dataframe()
                send_to_discord(table)
            else:
                print("[DEBUG] Brak nowych wpisów do zapisania.")

        time.sleep(60)

# --- Uruchomienie ---
if __name__ == "__main__":
    import threading
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
