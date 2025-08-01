import os
import re
import time
import io
import pandas as pd
import psycopg2
from ftplib import FTP
from flask import Flask
from tabulate import tabulate
import requests

# === KONFIGURACJA ===
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

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# === INICJALIZACJA BAZY ===
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            lock_type TEXT,
            success BOOLEAN,
            elapsed_time FLOAT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

# === PARSER LINII LOGA ===
def parse_log_content(content):
    pattern = r'\[LogMinigame\] \[LockpickingMinigame_C\] User: (\w+).*?Type: (\w+).*?Success: (\w+).*?Elapsed time: ([\d.]+)'
    matches = re.findall(pattern, content)
    print(f"[DEBUG] Rozpoznano {len(matches)} wpisów LockpickingMinigame.")
    return [(m[0], m[1], m[2] == "Yes", float(m[3])) for m in matches]

# === POŁĄCZENIE Z FTP ===
def connect_ftp():
    try:
        ftp = FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
        ftp.login(FTP_USER, FTP_PASS)
        print("[DEBUG] Połączono z FTP.")
        return ftp
    except Exception as e:
        print(f"[ERROR] Błąd połączenia FTP: {e}")
        return None

# === PRZETWARZANIE LOGÓW ===
def process_logs():
    ftp = connect_ftp()
    if not ftp:
        print("[DEBUG] Pominięto przetwarzanie logów — brak połączenia FTP.")
        return

    try:
        ftp.cwd(LOG_DIR)
        files = []
        ftp.retrlines("MLSD", lambda line: files.append(line.split(";")[-1].strip()))
        log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
        print(f"[DEBUG] Znaleziono {len(log_files)} plików gameplay_*.log")

        all_entries = []

        for filename in log_files:
            print(f"[DEBUG] Przetwarzanie pliku: {filename}")
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode("utf-16-le", errors="ignore")
            entries = parse_log_content(content)
            all_entries.extend(entries)

        ftp.quit()
        print(f"[DEBUG] Łącznie rozpoznano {len(all_entries)} wpisów.")

        if all_entries:
            insert_to_db(all_entries)
            df = create_dataframe()
            if df is not None:
                send_to_discord(df)
        else:
            print("[DEBUG] Brak nowych wpisów do zapisania.")

    except Exception as e:
        print(f"[ERROR] Błąd przetwarzania logów: {e}")

# === ZAPIS DO BAZY ===
def insert_to_db(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for nick, lock_type, success, elapsed in entries:
        cur.execute("INSERT INTO lockpick_stats (nick, lock_type, success, elapsed_time) VALUES (%s, %s, %s, %s)",
                    (nick, lock_type, success, elapsed))
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Zapisano {len(entries)} wpisów do bazy danych.")

# === TWORZENIE TABELI ===
def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpick_stats", conn)
    conn.close()

    if df.empty:
        print("[DEBUG] Baza danych jest pusta.")
        return None

    grouped = df.groupby(["nick", "lock_type"]).agg(
        total_attempts=pd.NamedAgg(column="success", aggfunc="count"),
        successes=pd.NamedAgg(column="success", aggfunc="sum"),
        fails=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        accuracy=pd.NamedAgg(column="success", aggfunc=lambda x: round(100 * x.sum() / len(x), 2)),
        avg_time=pd.NamedAgg(column="elapsed_time", aggfunc="mean")
    ).reset_index()

    grouped["avg_time"] = grouped["avg_time"].round(2)
    grouped.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    print("[DEBUG] Tabela wyników została utworzona.")
    return grouped

# === WYSYŁKA DO DISCORDA ===
def send_to_discord(df):
    table = tabulate(df, headers="keys", tablefmt="github", stralign="center", numalign="center")
    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[DEBUG] Tabela wysłana do Discorda.")
    else:
        print(f"[ERROR] Błąd wysyłki do Discorda: {response.status_code} {response.text}")

# === PĘTLA GŁÓWNA ===
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        process_logs()
        print("[DEBUG] Czekam 60 sekund...")
        time.sleep(60)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
