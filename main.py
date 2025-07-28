import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
from tabulate import tabulate
from flask import Flask
from io import BytesIO
from datetime import datetime
import requests

# --- Konfiguracja FTP i Webhook ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Konfiguracja bazy danych PostgreSQL ---
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- Flask ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"

# --- Funkcje pomocnicze ---

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)
    return ftp

def list_log_files(ftp):
    files = []
    ftp.retrlines('LIST', lambda line: files.append(line.split()[-1]))
    return [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

def parse_log_content(content):
    data = []
    lines = content.splitlines()
    for line in lines:
        if "[LogMinigame] [LockpickingMinigame_C]" in line:
            user_match = re.search(r"User:\s*(\w+)", line)
            lock_match = re.search(r"Lock:\s*(\w+)", line)
            success_match = re.search(r"Success:\s*(Yes|No)", line)
            time_match = re.search(r"Elapsed time:\s*([\d\.]+)", line)
            if user_match and lock_match and success_match and time_match:
                data.append({
                    "Nick": user_match.group(1),
                    "Zamek": lock_match.group(1),
                    "Sukces": success_match.group(1) == "Yes",
                    "Czas": float(time_match.group(1))
                })
    return data

def save_to_database(data):
    if not data:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            zamek TEXT,
            sukces BOOLEAN,
            czas REAL
        )
    """)
    for entry in data:
        cur.execute(
            "INSERT INTO lockpicking (nick, zamek, sukces, czas) VALUES (%s, %s, %s, %s)",
            (entry["Nick"], entry["Zamek"], entry["Sukces"], entry["Czas"])
        )
    conn.commit()
    cur.close()
    conn.close()

def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpicking", conn)
    conn.close()

    if df.empty:
        return None

    summary = (
        df.groupby(['Nick', 'Zamek'])
        .agg(
            Proby=('Sukces', 'count'),
            Udane=('Sukces', lambda x: x.sum()),
            Nieudane=('Sukces', lambda x: (~x).sum()),
            Skutecznosc=('Sukces', lambda x: f"{(x.sum() / len(x)) * 100:.1f}%"),
            SredniCzas=('Czas', 'mean')
        )
        .reset_index()
    )

    summary['SredniCzas'] = summary['SredniCzas'].map(lambda x: f"{x:.2f}s")
    return summary

def format_table(df):
    df_centered = df.applymap(lambda x: str(x).center(len(str(x)) + 2))
    return "```\n" + tabulate(df_centered, headers="keys", tablefmt="github") + "\n```"

def print_table_to_console(df):
    print("[DEBUG] Tabela wyników:")
    print(tabulate(df, headers="keys", tablefmt="grid", showindex=False))

def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return
    message = format_table(df)
    print_table_to_console(df)
    payload = {"content": message}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[INFO] Tabela wysłana do Discorda.")
    else:
        print(f"[ERROR] Nie udało się wysłać do Discorda: {response.status_code}")

# --- Główna pętla przetwarzania ---
def main_loop():
    print("[DEBUG] Start programu")
    try:
        print("[DEBUG] Nawiązywanie połączenia FTP...")
        ftp = connect_ftp()
        log_files = list_log_files(ftp)
        print(f"[DEBUG] Znaleziono plików: {len(log_files)}")
        all_data = []
        for filename in log_files:
            print(f"[DEBUG] Przetwarzanie: {filename}")
            buffer = BytesIO()
            ftp.retrbinary(f"RETR {filename}", buffer.write)
            content = buffer.getvalue().decode("utf-16-le", errors="ignore")
            data = parse_log_content(content)
            all_data.extend(data)
        ftp.quit()
        print(f"[DEBUG] Wszystkich wpisów: {len(all_data)}")
        save_to_database(all_data)
        df = create_dataframe()
        send_to_discord(df)
    except Exception as e:
        print(f"[ERROR] Wystąpił błąd: {e}")

if __name__ == "__main__":
    main_loop()
    app.run(host="0.0.0.0", port=3000)
