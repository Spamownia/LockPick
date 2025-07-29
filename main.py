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

# --- KONFIGURACJA FTP ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOGS_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# --- KONFIGURACJA WEBHOOK ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- KONFIGURACJA BAZY DANYCH ---
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- INICJALIZACJA FLASK ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- GLOBALNE ---
SEEN_ENTRIES = set()


def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOGS_PATH)
    return ftp


def fetch_log_files():
    with connect_ftp() as ftp:
        files = []
        ftp.retrlines('LIST', lambda line: files.append(line))
        log_files = []
        for line in files:
            parts = line.split()
            filename = parts[-1]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                log_files.append(filename)
        return log_files


def download_file(filename):
    with connect_ftp() as ftp:
        ftp.cwd(FTP_LOGS_PATH)
        content = []

        def handle_binary(more_data):
            content.append(more_data)

        ftp.retrbinary(f"RETR {filename}", callback=handle_binary)
        raw = b''.join(content)
        return raw.decode("utf-16-le", errors="ignore")


def parse_log_content(content):
    pattern = re.compile(
        r'\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>.+?) \(.+?\) Lock type: (?P<lock>.+?) Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d.]+)s'
    )
    entries = []
    for match in pattern.finditer(content):
        entry_id = match.group(0)
        if entry_id in SEEN_ENTRIES:
            continue
        SEEN_ENTRIES.add(entry_id)
        entries.append({
            "Nick": match.group("nick"),
            "Zamek": match.group("lock"),
            "Sukces": match.group("success") == "Yes",
            "Czas": float(match.group("time"))
        })
    return entries


def create_dataframe(entries):
    if not entries:
        return None

    df = pd.DataFrame(entries)
    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Proby=("Sukces", "count"),
        Udane=("Sukces", "sum"),
        Nieudane=("Sukces", lambda x: (~x).sum()),
        Skutecznosc=("Sukces", lambda x: round(100 * x.sum() / len(x), 2)),
        SredniCzas=("Czas", lambda x: round(x.mean(), 2))
    ).reset_index()

    grouped.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    return grouped


def send_to_discord(df):
    if df is None or df.empty:
        print("[INFO] Brak nowych danych do wysłania.")
        return

    table_str = tabulate(df, headers="keys", tablefmt="github", showindex=False, stralign="center", numalign="center")
    message = f"**Statystyki Lockpickingu**\n```{table_str}```"
    response = requests.post(WEBHOOK_URL, json={"content": message})
    if response.status_code == 204:
        print("[INFO] Tabela wysłana na Discord.")
    else:
        print(f"[ERROR] Nie udało się wysłać na Discord: {response.status_code} {response.text}")


def process_logs():
    print("[DEBUG] Start main_loop")
    while True:
        try:
            log_files = fetch_log_files()
            print(f"[DEBUG] Znaleziono plików: {len(log_files)}")
            all_entries = []

            for filename in log_files:
                print(f"[DEBUG] Przetwarzanie pliku: {filename}")
                content = download_file(filename)
                entries = parse_log_content(content)
                if entries:
                    print(f"[DEBUG] Nowych wpisów w {filename}: {len(entries)}")
                    all_entries.extend(entries)

            if all_entries:
                df = create_dataframe(all_entries)
                send_to_discord(df)
            else:
                print("[DEBUG] Brak nowych wpisów w logach.")

        except Exception as e:
            print(f"[ERROR] Wystąpił błąd: {e}")

        print("[DEBUG] Oczekiwanie 60s na kolejną iterację...")
        time.sleep(60)


def run_flask():
    app.run(host='0.0.0.0', port=3000)


if __name__ == "__main__":
    threading.Thread(target=process_logs, daemon=True).start()
    run_flask()
