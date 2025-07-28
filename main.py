import os
import io
import re
import ssl
import time
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from flask import Flask
from ftplib import FTP_TLS
from datetime import datetime

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
    "sslmode": "require",
}

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def initialize_db():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_logs (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            steam_id TEXT,
            success BOOLEAN,
            elapsed_time REAL,
            failed_attempts INTEGER,
            lock_type TEXT,
            timestamp TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def parse_log_content(content):
    entries = []
    pattern = re.compile(
        r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}: \[LogMinigame\] \[LockpickingMinigame_C\] "
        r"User: (?P<nick>.*?) \(\d+, (?P<steam_id>\d+)\)\. "
        r"Success: (?P<success>Yes|No)\. "
        r"Elapsed time: (?P<elapsed_time>[\d.]+)\. "
        r"Failed attempts: (?P<failed_attempts>\d+)\. .*? "
        r"Lock type: (?P<lock_type>\w+)\."
    )

    for match in pattern.finditer(content):
        data = match.groupdict()
        entries.append({
            "nick": data["nick"],
            "steam_id": data["steam_id"],
            "success": data["success"] == "Yes",
            "elapsed_time": float(data["elapsed_time"]),
            "failed_attempts": int(data["failed_attempts"]),
            "lock_type": data["lock_type"],
        })
    return entries

def download_and_parse_logs():
    print("[DEBUG] Nawiązywanie połączenia FTP...")
    ftps = FTP_TLS()
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.auth()
    ftps.prot_p()
    ftps.login(FTP_USER, FTP_PASS)
    ftps.cwd(LOG_DIR)

    print("[DEBUG] Pobieranie listy plików logów...")
    files = []
    ftps.retrlines('MLSD', lambda line: files.append(line))
    log_files = [line.split(';')[-1].strip() for line in files if "gameplay_" in line and line.endswith(".log")]

    print(f"[DEBUG] Znaleziono {len(log_files)} plików logów.")
    all_entries = []
    for filename in log_files:
        print(f"[DEBUG] Przetwarzanie pliku: {filename}")
        log_data = io.BytesIO()
        ftps.retrbinary(f"RETR {filename}", log_data.write)
        log_data.seek(0)
        content = log_data.read().decode("utf-16le", errors="ignore")
        entries = parse_log_content(content)
        print(f"[DEBUG] Rozpoznano {len(entries)} wpisów w {filename}.")
        all_entries.extend(entries)

    ftps.quit()
    return all_entries

def save_entries_to_db(entries):
    conn = connect_db()
    cur = conn.cursor()
    for e in entries:
        cur.execute("""
            INSERT INTO lockpicking_logs (nick, steam_id, success, elapsed_time, failed_attempts, lock_type, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (e["nick"], e["steam_id"], e["success"], e["elapsed_time"], e["failed_attempts"], e["lock_type"]))
    conn.commit()
    cur.close()
    conn.close()

def generate_and_send_table():
    conn = connect_db()
    df = pd.read_sql_query("SELECT * FROM lockpicking_logs", conn)
    conn.close()

    if df.empty:
        print("[INFO] Brak danych w bazie.")
        return

    grouped = df.groupby(["nick", "lock_type"]).agg(
        Wszystkie=('success', 'count'),
        Udane=('success', lambda x: sum(x)),
        Nieudane=('success', lambda x: len(x) - sum(x)),
        Skuteczność=('success', lambda x: f"{100 * sum(x)/len(x):.1f}%"),
        Średni_czas=('elapsed_time', lambda x: f"{x.mean():.2f}s")
    ).reset_index()

    table = tabulate(grouped, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    print("[DEBUG] Tabela gotowa do wysyłki:\n", table)

    payload = {
        "content": f"```\n{table}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    print("[INFO] Wysłano dane na webhook. Status:", response.status_code)

def main_loop():
    print("[DEBUG] Start programu")
    try:
        initialize_db()
        entries = download_and_parse_logs()
        if entries:
            save_entries_to_db(entries)
            generate_and_send_table()
        else:
            print("[INFO] Brak nowych wpisów do zapisania.")
    except Exception as e:
        print(f"[ERROR] Wystąpił błąd: {e}")

if __name__ == "__main__":
    main_loop()
    app.run(host="0.0.0.0", port=3000)
