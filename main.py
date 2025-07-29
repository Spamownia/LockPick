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

# --- Konfiguracja ---
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

CHECK_INTERVAL = 60
SEEN_LINES = set()

# --- Inicjalizacja Flask ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- Funkcje pomocnicze ---

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    print(f"[DEBUG] Połączono z FTP: {FTP_HOST}:{FTP_PORT}, katalog: {FTP_DIR}")
    return ftp

def list_log_files(ftp):
    lines = []
    ftp.retrlines("LIST", lines.append)
    files = []
    for line in lines:
        parts = line.split()
        filename = parts[-1]
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            files.append(filename)
    print(f"[DEBUG] Znaleziono pliki: {files}")
    return files

def download_logs():
    ftp = connect_ftp()
    log_files = list_log_files(ftp)
    logs = []
    for filename in log_files:
        print(f"[DEBUG] Pobieranie pliku: {filename}")
        content = []
        ftp.retrbinary(f"RETR {filename}", lambda data: content.append(data))
        try:
            log_data = b"".join(content).decode("utf-16le", errors="ignore")
        except Exception as e:
            print(f"[ERROR] Błąd dekodowania {filename}: {e}")
            continue
        logs.append((filename, log_data))
    ftp.quit()
    return logs

def parse_log_content(log_text):
    # Regex zgodny z formatem podanym
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>\w+) .*? Lock: (?P<lock>\w+).*? Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d.]+)",
        re.DOTALL
    )
    entries = []
    for match in pattern.finditer(log_text):
        raw_line = match.group(0)
        if raw_line in SEEN_LINES:
            continue
        SEEN_LINES.add(raw_line)
        entries.append({
            "Nick": match.group("nick"),
            "Zamek": match.group("lock"),
            "Sukces": match.group("success") == "Yes",
            "Czas": float(match.group("time"))
        })
    return entries

def save_to_db(entries):
    if not entries:
        print("[DEBUG] Brak nowych wpisów do zapisu w DB")
        return
    try:
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
        for entry in entries:
            cur.execute(
                "INSERT INTO lockpicking (nick, zamek, sukces, czas) VALUES (%s, %s, %s, %s)",
                (entry["Nick"], entry["Zamek"], entry["Sukces"], entry["Czas"])
            )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[DEBUG] Zapisano {len(entries)} nowych wpisów do bazy danych")
    except Exception as e:
        print(f"[ERROR] Błąd zapisu do DB: {e}")

def create_dataframe():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql_query("SELECT * FROM lockpicking", conn)
        conn.close()
    except Exception as e:
        print(f"[ERROR] Błąd odczytu z DB: {e}")
        return None
    if df.empty:
        print("[DEBUG] Brak danych w tabeli lockpicking")
        return None
    grouped = df.groupby(["nick", "zamek"]).agg(
        Proby=('sukces', 'count'),
        Udane=('sukces', 'sum'),
        Nieudane=('sukces', lambda x: (~x).sum()),
        SredniCzas=('czas', 'mean')
    ).reset_index()
    grouped['Skutecznosc'] = (grouped['Udane'] / grouped['Proby'] * 100).round(1)
    grouped['SredniCzas'] = grouped['SredniCzas'].round(2)
    return grouped

def send_to_discord(df):
    df.columns = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Średni czas", "Skuteczność"]
    for col in df.columns:
        df[col] = df[col].astype(str)
    tabela = tabulate(df.values.tolist(), headers=df.columns.tolist(), tablefmt="github", stralign="center", numalign="center")
    payload = {"content": f"```\n{tabela}\n```"}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        print(f"[DEBUG] Wysłano dane do Discorda, status: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Błąd wysyłki do Discorda: {e}")

def main_loop():
    print("[DEBUG] Start main_loop")
    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.utcnow().isoformat()} ---")
        try:
            logs = download_logs()
        except Exception as e:
            print(f"[ERROR] Błąd pobierania logów z FTP: {e}")
            time.sleep(CHECK_INTERVAL)
            continue

        total_new_entries = 0
        for filename, log_text in logs:
            print(f"[DEBUG] Przetwarzanie: {filename}, linie: {log_text.count(chr(10))}")
            new_entries = parse_log_content(log_text)
            if new_entries:
                total_new_entries += len(new_entries)
                save_to_db(new_entries)

        if total_new_entries > 0:
            df = create_dataframe()
            if df is not None:
                send_to_discord(df)
            else:
                print("[DEBUG] Brak danych do wysłania po zgrupowaniu")
        else:
            print("[DEBUG] Brak nowych wpisów w logach")

        time.sleep(CHECK_INTERVAL)

# --- Uruchomienie wątku pętli głównej ---
threading.Thread(target=main_loop, daemon=True).start()

# --- Uruchomienie serwera Flask ---
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000)
