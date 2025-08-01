import os
import re
import io
import time
import psycopg2
import pandas as pd
from flask import Flask
from datetime import datetime
from ftplib import FTP
from tabulate import tabulate

# --- Konfiguracja ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- Flask dla UptimeRobot ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- Funkcje pomocnicze ---
def connect_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)
    return ftp

def download_log_files():
    ftp = connect_ftp()
    filenames = []
    try:
        ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania listy plików: {e}")
        ftp.quit()
        return []

    log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
    logs = []

    for filename in log_files:
        print(f"[INFO] Pobieranie pliku: {filename}")
        try:
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            bio.seek(0)
            content = bio.read().decode("utf-16-le", errors="ignore")
            logs.append((filename, content))
        except Exception as e:
            print(f"[ERROR] Błąd pobierania pliku {filename}: {e}")
    ftp.quit()
    return logs

def parse_log_content(content):
    pattern = r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (.+?) \((.+?)\).*?Success: (Yes|No).*?Elapsed time: ([\d.]+)"
    matches = re.findall(pattern, content)

    parsed = []
    for match in matches:
        nick, lock_type, success, elapsed = match
        parsed.append({
            "nick": nick.strip(),
            "lock": lock_type.strip(),
            "success": success == "Yes",
            "elapsed": float(elapsed)
        })
    print(f"[DEBUG] Rozpoznano {len(parsed)} wpisów LockpickingMinigame_C")
    return parsed

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def initialize_db():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicking (
                    nick TEXT,
                    lock TEXT,
                    success BOOLEAN,
                    elapsed FLOAT,
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    print("[DEBUG] Baza danych zainicjalizowana")

def save_to_db(entries):
    with connect_db() as conn:
        with conn.cursor() as cur:
            for entry in entries:
                cur.execute("""
                    INSERT INTO lockpicking (nick, lock, success, elapsed)
                    VALUES (%s, %s, %s, %s)
                """, (entry["nick"], entry["lock"], entry["success"], entry["elapsed"]))
            conn.commit()
    print(f"[DEBUG] Zapisano {len(entries)} wpisów do bazy")

def create_dataframe():
    with connect_db() as conn:
        df = pd.read_sql_query("SELECT * FROM lockpicking", conn)

    if df.empty:
        print("[DEBUG] Brak danych w bazie")
        return None

    grouped = df.groupby(["nick", "lock"]).agg(
        total=("success", "count"),
        success_count=("success", "sum"),
        fail_count=("success", lambda x: (~x).sum()),
        success_rate=("success", lambda x: f"{round(x.mean() * 100)}%"),
        avg_time=("elapsed", "mean")
    ).reset_index()

    grouped["avg_time"] = grouped["avg_time"].map(lambda x: f"{x:.2f}s")
    grouped.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    print(f"[DEBUG] Tabela gotowa ({len(grouped)} wierszy)")
    return grouped

def send_to_discord(df):
    table = tabulate(df.values.tolist(), headers=df.columns, tablefmt="github", stralign="center")
    payload = {"content": f"```\n{table}\n```"}
    try:
        import requests
        response = requests.post(WEBHOOK_URL, json=payload)
        print(f"[DEBUG] Wysłano tabelę do Discorda. Status: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Błąd wysyłki na webhook: {e}")

# --- Główna pętla ---
def main_loop():
    print("[DEBUG] Start main_loop")
    initialize_db()
    processed = set()

    while True:
        logs = download_log_files()
        new_entries = []

        for filename, content in logs:
            if filename in processed:
                continue
            entries = parse_log_content(content)
            if entries:
                new_entries.extend(entries)
            processed.add(filename)

        if new_entries:
            print(f"[INFO] Znaleziono {len(new_entries)} nowych wpisów")
            save_to_db(new_entries)
            df = create_dataframe()
            if df is not None:
                send_to_discord(df)
        else:
            print("[INFO] Brak nowych wpisów w logach")

        time.sleep(60)

# --- Start ---
if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
