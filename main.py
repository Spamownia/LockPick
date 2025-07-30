import os
import re
import time
import threading
import datetime
from ftplib import FTP
from flask import Flask
from collections import defaultdict
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate

# --- Flask ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"

# --- Konfiguracje ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require",
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Inicjalizacja bazy ---
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            id SERIAL PRIMARY KEY,
            nickname TEXT,
            castle TEXT,
            success BOOLEAN,
            elapsed_time REAL,
            log_file TEXT,
            UNIQUE(nickname, castle, elapsed_time, log_file)
        )
    """)
    conn.commit()
    conn.close()

# --- Pobieranie logów ---
def download_logs():
    print("[DEBUG] Nawiązywanie połączenia FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    log_files = []
    ftp.retrlines("LIST", lambda line: log_files.append(line.split()[-1]))
    log_files = [f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików logów.")
    logs = {}

    for filename in log_files:
        lines = []
        ftp.retrbinary(f"RETR {filename}", lines.append)
        raw = b"".join(lines)
        logs[filename] = raw.decode("utf-16le", errors="ignore")

    ftp.quit()
    return logs

# --- Parsowanie logów ---
def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame\].+?User:\s*(.+?)\s.*?LockType:\s*(.+?)\..*?Success:\s*(Yes|No).*?Elapsed time:\s*([\d.]+)", re.DOTALL
    )
    results = []
    for match in pattern.finditer(content):
        user, lock_type, success, elapsed_time = match.groups()
        results.append({
            "nickname": user.strip(),
            "castle": lock_type.strip(),
            "success": success == "Yes",
            "elapsed_time": float(elapsed_time.rstrip("."))  # 🛠️ kluczowa poprawka
        })
    return results

# --- Zapis danych do DB ---
def save_to_db(entries, log_file):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_count = 0
    for e in entries:
        try:
            cur.execute("""
                INSERT INTO lockpicking (nickname, castle, success, elapsed_time, log_file)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (e["nickname"], e["castle"], e["success"], e["elapsed_time"], log_file))
            if cur.rowcount > 0:
                new_count += 1
        except Exception as ex:
            print(f"[ERROR] Błąd zapisu do bazy: {ex}")
    conn.commit()
    conn.close()
    return new_count

# --- Tabela wyników ---
def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpicking", conn)
    conn.close()
    if df.empty:
        return None

    grouped = df.groupby(["nickname", "castle"])
    result = []

    for (nickname, castle), group in grouped:
        total = len(group)
        success = group["success"].sum()
        fail = total - success
        efficiency = f"{(success / total) * 100:.2f}%"
        avg_time = f"{group['elapsed_time'].mean():.2f}s"
        result.append([nickname, castle, total, success, fail, efficiency, avg_time])

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = tabulate(result, headers=headers, tablefmt="grid", stralign="center", numalign="center")
    return table

# --- Wysyłka na webhook ---
def send_to_discord(table):
    if not table:
        print("[DEBUG] Brak danych do wysłania.")
        return
    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code != 204:
        print(f"[ERROR] Błąd wysyłki: {response.status_code} - {response.text}")
    else:
        print("[DEBUG] Wysłano dane na Discord.")

# --- Przetwarzanie logów ---
def process_logs():
    logs = download_logs()
    total_new = 0
    for filename, content in logs.items():
        entries = parse_log_content(content)
        new_count = save_to_db(entries, filename)
        total_new += new_count
        print(f"[DEBUG] Plik: {filename} | Znaleziono wpisów: {len(entries)} | Nowe: {new_count}")
    return total_new

# --- Pętla główna ---
def main_loop():
    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.datetime.utcnow().isoformat()} ---")
        new_entries = process_logs()
        if new_entries > 0:
            print(f"[DEBUG] Wykryto {new_entries} nowych wpisów.")
            table = create_dataframe()
            send_to_discord(table)
        else:
            print("[DEBUG] Brak nowych wpisów w logach.")
        time.sleep(60)

# --- Start ---
if __name__ == "__main__":
    print("[DEBUG] Start main_loop")
    init_db()
    threading.Thread(target=main_loop).start()
    app.run(host='0.0.0.0', port=3000)
