import os
import time
import re
from datetime import datetime
from ftplib import FTP
from io import BytesIO
import pandas as pd
import psycopg2
from tabulate import tabulate
import requests
from flask import Flask

# --- Flask App (Ping)
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"
# uruchomienie aplikacji przeniesione do końca

# --- Konfiguracja
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
LOG_PATTERN = "gameplay_"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- Połączenie z bazą
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

# --- Inicjalizacja tabeli
def init_db():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_history (
                    nick TEXT,
                    lock_type TEXT,
                    success BOOLEAN,
                    elapsed_time FLOAT,
                    timestamp TIMESTAMP
                )
            """)
            conn.commit()
    print("[DEBUG] Baza danych zainicjalizowana")

# --- Parsowanie logu
def parse_log_content(content):
    lines = content.splitlines()
    entries = []
    for line in lines:
        line = line.strip()
        if "[LogMinigame]" in line and "User:" in line and "Success:" in line:
            try:
                nick_match = re.search(r'User:\s*(\w+)', line)
                lock_match = re.search(r'Lock:\s*(\w+)', line)
                success_match = re.search(r'Success:\s*(Yes|No)', line)
                time_match = re.search(r'Elapsed time:\s*([\d.]+)', line)
                timestamp_match = re.search(r'^(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})', line)

                if all([nick_match, lock_match, success_match, time_match, timestamp_match]):
                    nick = nick_match.group(1)
                    lock_type = lock_match.group(1)
                    success = success_match.group(1) == "Yes"
                    elapsed_time = float(time_match.group(1))
                    timestamp = datetime.strptime(timestamp_match.group(1), "%Y.%m.%d-%H.%M.%S")

                    entries.append((nick, lock_type, success, elapsed_time, timestamp))
            except Exception as e:
                print(f"[ERROR] Błąd parsowania: {e}")
    print(f"[DEBUG] Sparsowano {len(entries)} wpisów z logu")
    return entries

# --- Pobranie i przetwarzanie logów
def fetch_and_process_logs():
    new_entries = []
    with FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(LOG_DIR)
        files = []
        ftp.retrlines("LIST", lambda x: files.append(x))
        log_files = [f.split()[-1] for f in files if LOG_PATTERN in f]

        print(f"[DEBUG] Znaleziono {len(log_files)} plików logów")

        for filename in log_files:
            print(f"[DEBUG] Pobieranie pliku: {filename}")
            bio = BytesIO()
            try:
                ftp.retrbinary(f"RETR {filename}", bio.write)
                bio.seek(0)
                content = bio.read().decode("utf-16-le")
                entries = parse_log_content(content)
                new_entries.extend(entries)
            except Exception as e:
                print(f"[ERROR] Nie można przetworzyć {filename}: {e}")

    print(f"[DEBUG] Łącznie nowych wpisów: {len(new_entries)}")
    return new_entries

# --- Zapis do bazy danych
def save_entries_to_db(entries):
    if not entries:
        return
    with connect_db() as conn:
        with conn.cursor() as cur:
            for entry in entries:
                cur.execute("""
                    INSERT INTO lockpick_history (nick, lock_type, success, elapsed_time, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                """, entry)
        conn.commit()
    print(f"[DEBUG] Zapisano {len(entries)} wpisów do bazy")

# --- Tworzenie tabeli
def create_dataframe():
    with connect_db() as conn:
        df = pd.read_sql_query("SELECT * FROM lockpick_history", conn)
    if df.empty:
        print("[DEBUG] Brak danych w tabeli lockpick_history")
        return None

    grouped = df.groupby(['nick', 'lock_type']).agg(
        total_attempts=pd.NamedAgg(column="success", aggfunc='count'),
        successes=pd.NamedAgg(column="success", aggfunc='sum'),
        fails=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        efficiency=pd.NamedAgg(column="success", aggfunc=lambda x: f"{round((x.sum()/x.count())*100, 1)}%"),
        avg_time=pd.NamedAgg(column="elapsed_time", aggfunc='mean')
    ).reset_index()

    grouped['avg_time'] = grouped['avg_time'].apply(lambda x: f"{x:.2f}s")
    grouped = grouped.rename(columns={
        "nick": "Nick",
        "lock_type": "Zamek",
        "total_attempts": "Ilość wszystkich prób",
        "successes": "Udane",
        "fails": "Nieudane",
        "efficiency": "Skuteczność",
        "avg_time": "Średni czas"
    })

    print(f"[DEBUG] Wygenerowano tabelę z {len(grouped)} wierszami")
    return grouped

# --- Wysyłka na webhook
def send_to_discord(df):
    if df is None:
        print("[DEBUG] Brak danych do wysyłki")
        return
    table = tabulate(df, headers="keys", tablefmt="grid", showindex=False, stralign="center", numalign="center")
    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Wysłano tabelę na Discord - Status: {response.status_code}")

# --- Główna pętla
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    processed = False
    while True:
        try:
            entries = fetch_and_process_logs()
            if entries:
                save_entries_to_db(entries)
                df = create_dataframe()
                send_to_discord(df)
                processed = True
            else:
                print("[DEBUG] Brak nowych wpisów w logach")
        except Exception as e:
            print(f"[ERROR] Wystąpił błąd: {e}")
        time.sleep(60)

# --- Uruchomienie
if __name__ == "__main__":
    import threading
    threading.Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
