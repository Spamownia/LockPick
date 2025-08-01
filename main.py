import os
import time
import re
import io
import pandas as pd
import psycopg2
from psycopg2 import sql
from ftplib import FTP
from flask import Flask
from tabulate import tabulate
import requests

# --- CONFIG ---
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

# --- FLASK ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- DATABASE INIT ---
def init_db():
    print("[DEBUG] Inicjalizacja bazy danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_stats (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            lock_type TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            timestamp TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DEBUG] Tabela gotowa.")

# --- FTP LOG FETCH ---
def fetch_log_files():
    print("[DEBUG] Łączenie z FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    filenames = []
    ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
    log_files = [name for name in filenames if name.startswith("gameplay_") and name.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików logów.")
    logs = []
    for file in log_files:
        buffer = io.BytesIO()
        ftp.retrbinary(f"RETR {file}", buffer.write)
        buffer.seek(0)
        content = buffer.read().decode("utf-16-le", errors="ignore")
        logs.append(content)
    ftp.quit()
    return logs

# --- PARSE LOG CONTENT ---
def parse_log_content(content):
    print("[DEBUG] Rozpoczynam analizę logu...")
    pattern = re.compile(
        r"\[LogMinigame\].*?User:\s*(\w+).*?Lock:\s*(\w+).*?Success:\s*(Yes|No).*?Elapsed time:\s*([\d.]+)",
        re.DOTALL
    )
    matches = pattern.findall(content)
    print(f"[DEBUG] Rozpoznano {len(matches)} wpisów w logu.")
    data = []
    for nick, lock_type, success, elapsed_time in matches:
        data.append({
            "nick": nick,
            "lock_type": lock_type,
            "success": success == "Yes",
            "elapsed_time": float(elapsed_time)
        })
    return data

# --- SAVE TO DB ---
def save_to_db(entries):
    if not entries:
        print("[DEBUG] Brak nowych danych do zapisania.")
        return 0
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_rows = 0
    for entry in entries:
        cur.execute("""
            SELECT 1 FROM lockpicking_stats 
            WHERE nick=%s AND lock_type=%s AND success=%s AND elapsed_time=%s
        """, (entry['nick'], entry['lock_type'], entry['success'], entry['elapsed_time']))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO lockpicking_stats (nick, lock_type, success, elapsed_time)
                VALUES (%s, %s, %s, %s)
            """, (entry['nick'], entry['lock_type'], entry['success'], entry['elapsed_time']))
            new_rows += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Zapisano {new_rows} nowych wpisów.")
    return new_rows

# --- GENERATE TABLE ---
def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM lockpicking_stats", conn)
    conn.close()
    if df.empty:
        return "Brak danych."
    df["success"] = df["success"].astype(bool)
    grouped = df.groupby(["nick", "lock_type"]).agg(
        attempts=("success", "count"),
        successes=("success", "sum"),
        fails=("success", lambda x: (~x).sum()),
        efficiency=("success", "mean"),
        avg_time=("elapsed_time", "mean")
    ).reset_index()
    grouped["efficiency"] = (grouped["efficiency"] * 100).round(1).astype(str) + "%"
    grouped["avg_time"] = grouped["avg_time"].round(2)
    grouped = grouped.rename(columns={
        "nick": "Nick", "lock_type": "Zamek", "attempts": "Ilość wszystkich prób",
        "successes": "Udane", "fails": "Nieudane", "efficiency": "Skuteczność", "avg_time": "Średni czas"
    })
    table = tabulate(grouped, headers="keys", tablefmt="github", stralign="center", numalign="center")
    return table

# --- DISCORD SEND ---
def send_to_discord(table):
    print("[DEBUG] Wysyłka danych do Discorda...")
    payload = {
        "content": f"```\n{table}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[DEBUG] Wysłano dane na Discorda.")
    else:
        print(f"[ERROR] Błąd wysyłki Discord: {response.status_code} - {response.text}")

# --- MAIN LOOP ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        print("[DEBUG] Pętla 60s...")
        logs = fetch_log_files()
        all_entries = []
        for content in logs:
            parsed = parse_log_content(content)
            all_entries.extend(parsed)
        new_count = save_to_db(all_entries)
        if new_count > 0:
            table = create_dataframe()
            send_to_discord(table)
        else:
            print("[DEBUG] Brak nowych wpisów.")
        time.sleep(60)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
