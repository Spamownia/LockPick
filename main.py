import os
import time
import re
import psycopg2
import pandas as pd
from tabulate import tabulate
from flask import Flask
from datetime import datetime
from ftplib import FTP
import requests

# --- Flask keep-alive ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

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

# --- Baza danych ---
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            castle TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_data(rows):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for row in rows:
        cur.execute("""
            INSERT INTO lockpicking (nick, castle, success, elapsed_time)
            VALUES (%s, %s, %s, %s)
        """, row)
    conn.commit()
    cur.close()
    conn.close()

# --- Logika FTP ---
def download_log_files():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)
    files = []
    ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
    log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    
    logs = {}
    for filename in log_files:
        try:
            with open(filename, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
            with open(filename, "r", encoding="utf-16-le") as f:
                logs[filename] = f.readlines()
            os.remove(filename)
        except Exception as e:
            print(f"[ERROR] Problem z plikiem {filename}: {e}")
    ftp.quit()
    return logs

# --- Parser logów ---
def parse_log_content(log_lines, last_processed_entries):
    pattern = re.compile(
        r"User:\s*(?P<nick>\w+).*?Lock type:\s*(?P<castle>\w+).*?Success:\s*(?P<success>\w+).*?Elapsed time:\s*(?P<time>[\d.]+)",
        re.IGNORECASE
    )
    new_entries = []
    for line in log_lines:
        if line in last_processed_entries:
            continue
        match = pattern.search(line)
        if match:
            nick = match.group("nick")
            castle = match.group("castle")
            success = match.group("success").lower() == "yes"
            elapsed = float(match.group("time"))
            new_entries.append((nick, castle, success, elapsed))
            last_processed_entries.add(line)
    return new_entries

# --- Generowanie tabeli ---
def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM lockpicking", conn)
    conn.close()
    
    if df.empty:
        return "Brak danych"

    grouped = df.groupby(["nick", "castle"]).agg(
        Proby=('success', 'count'),
        Udane=('success', 'sum'),
        Nieudane=('success', lambda x: (~x).sum()),
        Skutecznosc=('success', lambda x: f"{x.mean() * 100:.1f}%"),
        SredniCzas=('elapsed_time', lambda x: f"{x.mean():.2f}s")
    ).reset_index()

    table = tabulate(
        grouped,
        headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"],
        tablefmt="grid",
        stralign="center",
        numalign="center"
    )

    print("[DEBUG] Wygenerowana tabela statystyk:")
    print(table)
    return f"```\n{table}\n```"

# --- Webhook ---
def send_to_discord(message):
    data = {"content": message}
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code != 204:
        print(f"[ERROR] Webhook status: {response.status_code}")
    else:
        print("[DEBUG] Wysłano dane na webhook.")

# --- Główna pętla ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    last_processed_entries = set()

    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.utcnow().isoformat()} ---")
        logs = download_log_files()
        all_new_entries = []

        for filename, lines in logs.items():
            print(f"[DEBUG] Przetwarzanie: {filename}, linie: {len(lines)}")
            new_entries = parse_log_content(lines, last_processed_entries)
            print(f"[DEBUG] Nowe wpisy: {len(new_entries)}")
            if new_entries:
                all_new_entries.extend(new_entries)

        if all_new_entries:
            insert_data(all_new_entries)
            tabela = create_dataframe()
            send_to_discord(tabela)
        else:
            print("[DEBUG] Brak nowych zdarzeń w logach.")

        time.sleep(60)

# --- Start aplikacji ---
if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
