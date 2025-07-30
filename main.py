import os
import time
import re
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from ftplib import FTP
from flask import Flask
from io import StringIO

# --- Konfiguracja ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- Flask: Ping ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"
# (Flask uruchamiany na końcu)

# --- Inicjalizacja DB ---
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_logs (
            log_id TEXT PRIMARY KEY,
            nick TEXT,
            zamek TEXT,
            success BOOLEAN,
            elapsed_time FLOAT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

# --- Parsowanie wpisów z logów ---
def parse_log_content(content):
    pattern = re.compile(
        r'\[LogMinigame\]\s+\[LockpickingMinigame_C\]\s+User:\s+(?P<nick>.*?)\s+\(.*?\)\.\s+Success:\s+(?P<success>Yes|No)\.\s+Elapsed time:\s+(?P<elapsed>[\d.]+).*?Lock type:\s+(?P<zamek>\w+)',
        re.MULTILINE
    )
    entries = []
    for match in pattern.finditer(content):
        nick = match.group("nick").strip()
        zamek = match.group("zamek").strip()
        success = match.group("success") == "Yes"
        elapsed = float(match.group("elapsed"))
        log_id = f"{nick}_{zamek}_{success}_{elapsed}"
        entries.append({
            "log_id": log_id,
            "nick": nick,
            "zamek": zamek,
            "success": success,
            "elapsed_time": elapsed
        })
    print(f"[DEBUG] Rozpoznano wpisów: {len(entries)}")
    return entries

# --- Pobierz pliki z FTP ---
def download_log_files():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)
    filenames = []
    ftp.retrlines("LIST", lambda x: filenames.append(x.split()[-1]))
    log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
    logs = {}
    for filename in log_files:
        with StringIO() as s:
            ftp.retrbinary(f"RETR {filename}", lambda data: s.write(data.decode('utf-16-le')))
            logs[filename] = s.getvalue()
    ftp.quit()
    print(f"[DEBUG] Łącznie pobrano {len(logs)} plików logów z FTP.")
    return logs

# --- Zapisz do DB i sprawdź nowe ---
def save_new_entries_to_db(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_entries = 0
    for entry in entries:
        try:
            cur.execute("INSERT INTO lockpicking_logs (log_id, nick, zamek, success, elapsed_time) VALUES (%s, %s, %s, %s, %s)",
                        (entry["log_id"], entry["nick"], entry["zamek"], entry["success"], entry["elapsed_time"]))
            new_entries += 1
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            continue
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Nowe wpisy zapisane: {new_entries}")
    return new_entries > 0

# --- Tworzenie tabeli wyników ---
def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM lockpicking_logs", conn)
    conn.close()
    if df.empty:
        return None
    grouped = df.groupby(["nick", "zamek"]).agg(
        wszystkie_próby=("success", "count"),
        udane=("success", "sum"),
        nieudane=("success", lambda x: (~x).sum()),
        skutecznosc=("success", lambda x: round(100 * x.sum() / len(x), 1)),
        średni_czas=("elapsed_time", "mean")
    ).reset_index()
    grouped["średni_czas"] = grouped["średni_czas"].round(2)
    return grouped

# --- Wysyłanie do Discorda ---
def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return
    table = tabulate(df, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="grid", stralign="center", numalign="center")
    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.ok:
        print("[DEBUG] Tabela wysłana na Discord.")
    else:
        print(f"[ERROR] Błąd wysyłania do Discord: {response.status_code} - {response.text}")

# --- Główna pętla ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        try:
            logs = download_log_files()
            all_entries = []
            for content in logs.values():
                all_entries.extend(parse_log_content(content))
            if save_new_entries_to_db(all_entries):
                df = create_dataframe()
                send_to_discord(df)
            else:
                print("[DEBUG] Brak nowych wpisów.")
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(60)

# --- Uruchomienie ---
if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
