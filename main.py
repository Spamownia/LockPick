import os
import re
import time
import ftplib
import psycopg2
import threading
import pandas as pd
import requests
from tabulate import tabulate
from datetime import datetime
from flask import Flask

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
CHECK_INTERVAL = 60  # sekund

# --- FLASK ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"

def run_flask():
    app.run(host='0.0.0.0', port=3000)

# --- BAZA DANYCH ---
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_logs (
                id SERIAL PRIMARY KEY,
                nick TEXT,
                lock_type TEXT,
                success BOOLEAN,
                elapsed_time FLOAT,
                timestamp TIMESTAMP DEFAULT NOW()
            );
        """)
    conn.commit()
    conn.close()

def insert_entries(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        for entry in entries:
            cur.execute("""
                INSERT INTO lockpicking_logs (nick, lock_type, success, elapsed_time)
                VALUES (%s, %s, %s, %s);
            """, (entry['nick'], entry['lock_type'], entry['success'], entry['elapsed_time']))
    conn.commit()
    conn.close()

# --- FTP ---
def get_latest_log_file():
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_PATH)
        files = []
        ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
        gameplay_logs = sorted(f for f in files if f.startswith("gameplay_") and f.endswith(".log"))
        if not gameplay_logs:
            print("[INFO] Brak plików gameplay_*.log")
            return None, None
        latest = gameplay_logs[-1]
        print(f"[INFO] Najnowszy log: {latest}")
        content = []
        ftp.retrbinary(f"RETR {latest}", content.append)
        return latest, b''.join(content).decode("utf-16-le", errors="ignore")

# --- PARSER ---
def parse_log_content(content):
    pattern = re.compile(
        r'\[LogMinigame\].+?User: (?P<nick>\w+).+?Type: (?P<lock_type>\w+).+?Success: (?P<success>Yes|No).+?Elapsed time: (?P<elapsed_time>\d+\.\d+)',
        re.DOTALL
    )
    return [
        {
            "nick": m.group("nick"),
            "lock_type": m.group("lock_type"),
            "success": m.group("success") == "Yes",
            "elapsed_time": float(m.group("elapsed_time"))
        }
        for m in pattern.finditer(content)
    ]

# --- TABELA ---
def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpicking_logs", conn)
    conn.close()
    if df.empty:
        return None
    grouped = df.groupby(["nick", "lock_type"]).agg(
        attempts=("success", "count"),
        successes=("success", "sum"),
        failures=("success", lambda x: (~x).sum()),
        accuracy=("success", "mean"),
        avg_time=("elapsed_time", "mean")
    ).reset_index()
    grouped["accuracy"] = (grouped["accuracy"] * 100).round(1).astype(str) + "%"
    grouped["avg_time"] = grouped["avg_time"].round(2)
    return grouped

# --- WEBHOOK ---
def send_to_discord(df):
    if df is None or df.empty:
        print("[INFO] Brak danych do wysłania.")
        return
    df.columns = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = tabulate(df, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table}\n```"})
    print("[INFO] Wysłano dane na webhook.")

# --- PĘTLA GŁÓWNA ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    last_seen = ""
    last_line_count = 0
    while True:
        try:
            log_name, log_content = get_latest_log_file()
            if log_name is None:
                time.sleep(CHECK_INTERVAL)
                continue
            lines = log_content.splitlines()
            if log_name != last_seen:
                print("[INFO] Nowy plik logu – pełna analiza.")
                entries = parse_log_content(log_content)
                insert_entries(entries)
                df = create_dataframe()
                send_to_discord(df)
                last_seen = log_name
                last_line_count = len(lines)
            elif len(lines) > last_line_count:
                print(f"[INFO] Nowe linie w {log_name}: {len(lines) - last_line_count}")
                new_content = "\n".join(lines[last_line_count:])
                entries = parse_log_content(new_content)
                if entries:
                    insert_entries(entries)
                    df = create_dataframe()
                    send_to_discord(df)
                else:
                    print("[INFO] Brak nowych wpisów lockpicking.")
                last_line_count = len(lines)
            else:
                print("[INFO] Brak nowych danych.")
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(CHECK_INTERVAL)

# --- START ---
if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    main_loop()
