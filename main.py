import os
import re
import time
import threading
import datetime
import pandas as pd
import psycopg2
import requests
from flask import Flask
from tabulate import tabulate
from io import StringIO
from ftplib import FTP_TLS

# --- KONFIGURACJA ---
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

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS lockpicking (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            lock_type TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            log_filename TEXT,
            log_line TEXT,
            timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()

def connect_ftp():
    print("[DEBUG] Nawiązywanie połączenia FTP...")
    ftps = FTP_TLS()
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.login(FTP_USER, FTP_PASS)
    ftps.prot_p()
    ftps.cwd(FTP_LOG_DIR)
    print("[DEBUG] Połączono z FTP")
    return ftps

def download_log_files():
    ftps = connect_ftp()
    files = []
    ftps.retrlines('LIST', lambda line: files.append(line))
    log_files = []
    for line in files:
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            filename = parts[8]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                log_files.append(filename)

    contents = {}
    for filename in log_files:
        print(f"[DEBUG] Pobieranie: {filename}")
        bio = StringIO()
        ftps.retrbinary(f"RETR {filename}", lambda data: bio.write(data.decode('utf-16-le')))
        contents[filename] = bio.getvalue()
    ftps.quit()
    return contents

def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (\w+).*?Type: (\w+).*?Success: (Yes|No).*?Elapsed time: ([\d.]+)",
        re.DOTALL
    )
    entries = []
    for match in pattern.finditer(content):
        nick, lock_type, success, elapsed_time = match.groups()
        entries.append({
            "nick": nick,
            "lock_type": lock_type,
            "success": success == "Yes",
            "elapsed_time": float(elapsed_time.rstrip('.')),  # Poprawka: usuwa końcową kropkę
            "log_line": match.group(0)
        })
    print(f"[DEBUG] Rozpoznano wpisów: {len(entries)}")
    return entries

def save_new_entries(entries, log_filename):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_entries = []
    for e in entries:
        cur.execute("SELECT 1 FROM lockpicking WHERE log_line = %s AND log_filename = %s", (e["log_line"], log_filename))
        if cur.fetchone():
            continue
        cur.execute("""
            INSERT INTO lockpicking (nick, lock_type, success, elapsed_time, log_filename, log_line)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (e["nick"], e["lock_type"], e["success"], e["elapsed_time"], log_filename, e["log_line"]))
        new_entries.append(e)
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Zapisano nowych wpisów: {len(new_entries)}")
    return new_entries

def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpicking", conn)
    conn.close()

    if df.empty:
        return None

    grouped = df.groupby(["nick", "lock_type"]).agg(
        total_attempts=pd.NamedAgg(column="success", aggfunc="count"),
        successful=pd.NamedAgg(column="success", aggfunc="sum"),
        failed=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        effectiveness=pd.NamedAgg(column="success", aggfunc=lambda x: round(100 * x.sum() / len(x), 2)),
        avg_time=pd.NamedAgg(column="elapsed_time", aggfunc=lambda x: round(x.mean(), 2))
    ).reset_index()

    grouped.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    return grouped

def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return

    table_str = tabulate(df, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    print("[DEBUG] Wysyłanie tabeli:\n", table_str)

    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table_str}\n```"})
    print(f"[DEBUG] Webhook response: {response.status_code}")

def process_logs():
    logs = download_log_files()
    all_new_entries = []
    for filename, content in logs.items():
        entries = parse_log_content(content)
        new_entries = save_new_entries(entries, filename)
        all_new_entries.extend(new_entries)
    return all_new_entries

def main_loop():
    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.datetime.utcnow().isoformat()} ---")
        new_entries = process_logs()
        if new_entries:
            print(f"[DEBUG] Nowe wpisy: {len(new_entries)} — generowanie tabeli...")
            df = create_dataframe()
            send_to_discord(df)
        else:
            print("[DEBUG] Brak nowych wpisów.")
        time.sleep(60)

if __name__ == "__main__":
    init_db()
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
