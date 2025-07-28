import os
import re
import time
import pandas as pd
import psycopg2
import requests
from io import StringIO
from tabulate import tabulate
from flask import Flask
from datetime import datetime
from ftplib import FTP

# --- KONFIGURACJA ---
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

app = Flask(__name__)
processed_files = set()

@app.route('/')
def index():
    return "Alive"

# --- LOGOWANIE DO BAZY ---
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS lockpick_stats (
                    nick TEXT,
                    lock_type TEXT,
                    success BOOLEAN,
                    elapsed_time FLOAT,
                    timestamp TIMESTAMP DEFAULT NOW()
                );
            ''')
            conn.commit()
    print("[DEBUG] Baza danych gotowa.")

# --- PARSOWANIE LOGÓW ---
def parse_log_content(content):
    pattern = re.compile(
        r'User: (?P<nick>.*?) .*?Lock: (?P<lock>.*?) .*?Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[0-9.]+)s',
        re.DOTALL
    )
    return [
        {
            "nick": m.group("nick"),
            "lock_type": m.group("lock"),
            "success": m.group("success") == "Yes",
            "elapsed_time": float(m.group("time"))
        }
        for m in pattern.finditer(content)
    ]

# --- POBIERANIE I PRZETWARZANIE LOGÓW ---
def fetch_log_files():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    filenames = []
    ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
    log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]

    print(f"[DEBUG] Znaleziono pliki logów: {log_files}")
    entries = []

    for filename in log_files:
        if filename in processed_files:
            continue

        print(f"[DEBUG] Przetwarzanie pliku: {filename}")
        bio = StringIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        content = bio.getvalue().encode('latin1').decode('utf-16-le')
        parsed = parse_log_content(content)
        if parsed:
            entries.extend(parsed)
            processed_files.add(filename)

    ftp.quit()
    return entries

# --- ZAPIS DO BAZY ---
def save_to_db(entries):
    if not entries:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            for e in entries:
                cur.execute('''
                    INSERT INTO lockpick_stats (nick, lock_type, success, elapsed_time)
                    VALUES (%s, %s, %s, %s)
                ''', (e["nick"], e["lock_type"], e["success"], e["elapsed_time"]))
            conn.commit()
    print(f"[DEBUG] Zapisano {len(entries)} wpisów do bazy.")

# --- TWORZENIE TABELI ---
def create_table():
    with get_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM lockpick_stats", conn)

    if df.empty:
        return None

    grouped = df.groupby(["nick", "lock_type"])
    table = []

    for (nick, lock_type), group in grouped:
        total = len(group)
        successes = group["success"].sum()
        failures = total - successes
        success_rate = f"{(successes / total) * 100:.1f}%"
        avg_time = f"{group['elapsed_time'].mean():.2f}s"
        table.append([nick, lock_type, total, successes, failures, success_rate, avg_time])

    headers = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    df_result = pd.DataFrame(table, columns=headers)

    print("[DEBUG] Tabela gotowa:")
    print(tabulate(df_result, headers="keys", tablefmt="github", stralign="center"))

    return tabulate(df_result, headers="keys", tablefmt="github", stralign="center")

# --- WYSYŁANIE NA DISCORD ---
def send_to_discord(table):
    if table:
        data = {"content": f"```\n{table}\n```"}
        response = requests.post(WEBHOOK_URL, json=data)
        print(f"[DEBUG] Wysłano na Discord: {response.status_code}")
    else:
        print("[DEBUG] Brak danych do wysłania.")

# --- GŁÓWNA PĘTLA ---
def main_loop():
    print("[DEBUG] Start programu")
    init_db()

    while True:
        entries = fetch_log_files()
        if entries:
            save_to_db(entries)
            table = create_table()
            send_to_discord(table)
        else:
            print("[DEBUG] Brak nowych logów.")
        time.sleep(60)

# --- START ---
if __name__ == "__main__":
    import threading
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
