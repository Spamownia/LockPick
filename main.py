import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
from tabulate import tabulate
from datetime import datetime
from flask import Flask
import threading
import requests

# === KONFIGURACJA ===
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
    "sslmode": "require"
}

CHECK_INTERVAL = 60  # sekundy

# === FUNKCJE POMOCNICZE ===

def connect_to_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    return ftp

def list_log_files(ftp):
    files = []
    ftp.retrlines('LIST', lambda line: files.append(line.split()[-1]))
    return [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

def download_log_file(ftp, filename):
    content = []
    ftp.retrbinary(f"RETR {filename}", lambda data: content.append(data))
    return b''.join(content).decode('utf-16-le', errors='ignore')

def parse_log_content(content):
    entries = []
    pattern = re.compile(r"\[LogMinigame\].*?User:\s*(\w+).*?Type:\s*(\w+).*?Success:\s*(Yes|No).*?Elapsed time:\s*([0-9.]+)", re.DOTALL)
    for match in pattern.finditer(content):
        nick, lock_type, success, elapsed = match.groups()
        entries.append({
            "Nick": nick,
            "Zamek": lock_type,
            "Sukces": success == "Yes",
            "Czas": float(elapsed)
        })
    return entries

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking (
                nick TEXT,
                zamek TEXT,
                sukces BOOLEAN,
                czas FLOAT,
                unikalny_id TEXT PRIMARY KEY
            );
        """)
        conn.commit()
    conn.close()

def entry_id(entry):
    return f"{entry['Nick']}_{entry['Zamek']}_{entry['Czas']}"

def save_entries_to_db(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    new_count = 0
    with conn.cursor() as cur:
        for entry in entries:
            uid = entry_id(entry)
            try:
                cur.execute("""
                    INSERT INTO lockpicking (nick, zamek, sukces, czas, unikalny_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (entry['Nick'], entry['Zamek'], entry['Sukces'], entry['Czas'], uid))
                new_count += 1
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                continue
        conn.commit()
    conn.close()
    return new_count

def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpicking", conn)
    conn.close()
    if df.empty:
        return None

    grouped = df.groupby(['nick', 'zamek']).agg(
        Ilosc_prob=('sukces', 'count'),
        Udane=('sukces', 'sum'),
        Nieudane=('sukces', lambda x: (~x).sum()),
        Skutecznosc=('sukces', lambda x: f"{(x.sum() / len(x) * 100):.2f}%"),
        Sredni_czas=('czas', lambda x: f"{x.mean():.2f}s")
    ).reset_index()

    return grouped

def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return
    table = tabulate(df, headers='keys', tablefmt='grid', stralign='center', numalign='center')
    content = f"```\n{table}\n```"
    requests.post(WEBHOOK_URL, json={"content": content})
    print("[DEBUG] Wysłano dane na Discord.")

# === GŁÓWNA PĘTLA ===

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    seen_files = set()

    while True:
        try:
            ftp = connect_to_ftp()
            files = list_log_files(ftp)
            new_entries = []
            print(f"[DEBUG] Znalezione pliki: {files}")

            for file in files:
                if file not in seen_files:
                    print(f"[DEBUG] Przetwarzanie pliku: {file}")
                    content = download_log_file(ftp, file)
                    entries = parse_log_content(content)
                    new_entries.extend(entries)
                    seen_files.add(file)

            ftp.quit()

            if new_entries:
                print(f"[DEBUG] Liczba nowych wpisów: {len(new_entries)}")
                added = save_entries_to_db(new_entries)
                print(f"[DEBUG] Dodano {added} nowych wpisów do bazy.")
                df = create_dataframe()
                send_to_discord(df)
            else:
                print("[DEBUG] Brak nowych wpisów.")

        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(CHECK_INTERVAL)

# === SERWER FLASK ===

app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

if __name__ == "__main__":
    threading.Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
