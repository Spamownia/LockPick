import os
import time
import re
import ftplib
import io
import pandas as pd
import psycopg2
import requests
from datetime import datetime
from flask import Flask
from tabulate import tabulate

# --- Flask do pingowania ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"
def start_flask():
    import threading
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=3000)).start()

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
    "sslmode": "require",
}

# --- Parsowanie loga ---
def parse_log_content(content):
    pattern = r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (.*?) \((.*?)\).*?Success: (Yes|No).*?Elapsed time: ([\d.]+)"
    matches = re.findall(pattern, content, re.DOTALL)
    data = []
    for match in matches:
        nick, lock_type, success, time_elapsed = match
        data.append({
            "Nick": nick.strip(),
            "LockType": lock_type.strip(),
            "Success": success.strip() == "Yes",
            "Time": float(time_elapsed),
        })
    return data

# --- Inicjalizacja DB ---
def init_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicks (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    lock_type TEXT,
                    success BOOLEAN,
                    time FLOAT,
                    log_name TEXT,
                    UNIQUE (nick, lock_type, success, time, log_name)
                )
            """)
    print("[DEBUG] Baza danych gotowa")

# --- Zapis danych do DB ---
def save_to_db(entries, log_name):
    if not entries:
        return
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for entry in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpicks (nick, lock_type, success, time, log_name)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (entry["Nick"], entry["LockType"], entry["Success"], entry["Time"], log_name))
                except Exception as e:
                    print(f"[ERROR] DB insert: {e}")

# --- Generowanie tabeli ---
def create_dataframe():
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql("SELECT * FROM lockpicks", conn)

    if df.empty:
        print("[DEBUG] Brak danych do utworzenia tabeli.")
        return None

    grouped = df.groupby(["Nick", "LockType"], observed=False)
    summary = grouped.agg(
        Attempts=("Success", "count"),
        Successes=("Success", "sum"),
        Failures=("Success", lambda x: (~x).sum()),
        Accuracy=("Success", lambda x: f"{(x.sum() / len(x) * 100):.1f}%"),
        AvgTime=("Time", "mean")
    ).reset_index()

    summary["AvgTime"] = summary["AvgTime"].apply(lambda x: f"{x:.2f}s")
    return summary

# --- Wysyłka na Discorda ---
def send_to_discord(df):
    if df is None or df.empty:
        return

    table_str = tabulate(df, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="grid", stralign="center", numalign="center")
    print("[DEBUG] Tabela wygenerowana:\n", table_str)

    requests.post(WEBHOOK_URL, json={"content": f"```\n{table_str}\n```"})

# --- Obsługa FTP i logów ---
def get_latest_log_name(ftp):
    filenames = []
    ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
    log_files = sorted([f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")])
    return log_files[-1] if log_files else None

def read_log(ftp, filename):
    content_io = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", content_io.write)
    return content_io.getvalue().decode("utf-16-le", errors="ignore")

def check_for_new_entries(ftp, filename, last_line_count):
    content = read_log(ftp, filename)
    entries = parse_log_content(content)
    if len(entries) > last_line_count:
        new_entries = entries[last_line_count:]
        print(f"[DEBUG] W pliku {filename} znaleziono {len(new_entries)} nowych wpisów")
        return new_entries, len(entries)
    else:
        print(f"[DEBUG] Brak nowych wpisów w {filename}")
        return [], len(entries)

# --- Główna pętla ---
def main_loop():
    print("[DEBUG] Start programu")
    init_db()

    last_line_count = 0
    last_filename = None

    while True:
        try:
            with ftplib.FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT)
                ftp.login(FTP_USER, FTP_PASS)
                ftp.cwd(FTP_DIR)

                filename = get_latest_log_name(ftp)
                if filename:
                    if filename != last_filename:
                        last_line_count = 0
                        last_filename = filename

                    new_entries, new_count = check_for_new_entries(ftp, filename, last_line_count)
                    if new_entries:
                        save_to_db(new_entries, filename)
                        df = create_dataframe()
                        send_to_discord(df)
                    last_line_count = new_count
        except Exception as e:
            print(f"[ERROR] W pętli głównej: {e}")

        time.sleep(60)

if __name__ == "__main__":
    start_flask()
    main_loop()
