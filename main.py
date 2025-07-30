import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
import datetime
import requests
from flask import Flask
from io import BytesIO
from tabulate import tabulate

# === KONFIGURACJE ===

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

app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"


# === FUNKCJE ===

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_logs (
            id SERIAL PRIMARY KEY,
            nickname TEXT,
            lock_type TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            timestamp TIMESTAMPTZ DEFAULT now(),
            UNIQUE(nickname, lock_type, success, elapsed_time)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DEBUG] Baza danych zainicjalizowana")

def fetch_log_files():
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)
        filenames = []
        ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
        log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
        print(f"[DEBUG] Znaleziono {len(log_files)} plików logów")
        return log_files

def download_file(filename):
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)
        bio = BytesIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        bio.seek(0)
        return bio.read().decode("utf-16-le")

def parse_log_content(content):
    entries = []
    pattern = r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (.*?) \((.*?)\).*?Success: (Yes|No).*?Elapsed time: ([0-9.]+)"
    for match in re.finditer(pattern, content):
        nickname, lock_type, success, elapsed_time = match.groups()
        entries.append({
            "nickname": nickname.strip(),
            "lock_type": lock_type.strip(),
            "success": success == "Yes",
            "elapsed_time": float(elapsed_time)
        })
    return entries

def insert_entries(entries):
    if not entries:
        return 0
    conn = connect_db()
    cur = conn.cursor()
    new_count = 0
    for e in entries:
        try:
            cur.execute("""
                INSERT INTO lockpicking_logs (nickname, lock_type, success, elapsed_time)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (e["nickname"], e["lock_type"], e["success"], e["elapsed_time"]))
            if cur.rowcount > 0:
                new_count += 1
        except Exception as ex:
            print(f"[ERROR] Wstawianie wpisu nie powiodło się: {ex}")
    conn.commit()
    cur.close()
    conn.close()
    return new_count

def create_dataframe():
    conn = connect_db()
    df = pd.read_sql_query("SELECT * FROM lockpicking_logs", conn)
    conn.close()
    if df.empty:
        return None
    summary = df.groupby(["nickname", "lock_type"]).agg(
        total_attempts=pd.NamedAgg(column="success", aggfunc="count"),
        successful=pd.NamedAgg(column="success", aggfunc="sum"),
        failed=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        efficiency=pd.NamedAgg(column="success", aggfunc=lambda x: f"{(x.sum()/len(x))*100:.1f}%"),
        avg_time=pd.NamedAgg(column="elapsed_time", aggfunc="mean")
    ).reset_index()

    summary["avg_time"] = summary["avg_time"].map(lambda x: f"{x:.2f}s")
    return summary

def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania")
        return
    table = tabulate(df, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="grid", stralign="center", numalign="center")
    payload = {"content": f"```\n{table}\n```"}
    r = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Wysłano dane na webhook: {r.status_code}")

def process_logs():
    log_files = fetch_log_files()
    total_new_entries = 0
    for filename in log_files:
        print(f"[DEBUG] Przetwarzanie: {filename}")
        content = download_file(filename)
        entries = parse_log_content(content)
        new_entries = insert_entries(entries)
        print(f"[DEBUG] Nowe wpisy: {new_entries}")
        total_new_entries += new_entries
    return total_new_entries

def main_loop():
    init_db()
    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.datetime.utcnow().isoformat()} ---")
        new_entries = process_logs()
        if new_entries > 0:
            df = create_dataframe()
            send_to_discord(df)
        else:
            print("[DEBUG] Brak nowych wpisów")
        time.sleep(CHECK_INTERVAL)

# === START ===
if __name__ == "__main__":
    import threading
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
