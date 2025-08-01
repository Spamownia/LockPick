import os
import re
import time
import pandas as pd
import psycopg2
import requests
from datetime import datetime
from tabulate import tabulate
from flask import Flask

# --- KONFIGURACJA ---
FTP_LOG_PATH = "./logs"  # lokalna ścieżka do logów po pobraniu z FTP
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

# --- FUNKCJE ---
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            lock_type TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            log_hash TEXT UNIQUE
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def hash_log_line(line):
    return str(abs(hash(line)))

def parse_log_content(content):
    results = []
    pattern = re.compile(r'\[LogMinigame\].*?User: (.+?) .*?Type: (.+?) .*?Success: (Yes|No).*?Elapsed time: ([\d.]+)')
    for line in content.splitlines():
        match = pattern.search(line)
        if match:
            nick, lock_type, success, elapsed = match.groups()
            log_hash = hash_log_line(line)
            results.append({
                "nick": nick.strip(),
                "lock_type": lock_type.strip(),
                "success": success == "Yes",
                "elapsed_time": float(elapsed),
                "log_hash": log_hash
            })
        else:
            print(f"[DEBUG] Pominięto linię (nie pasuje): {line.strip()}")
    print(f"[DEBUG] Rozpoznano {len(results)} wpisów z logu.")
    return results

def insert_to_db(records):
    if not records:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_records = 0
    for r in records:
        try:
            cur.execute("""
                INSERT INTO lockpick_logs (nick, lock_type, success, elapsed_time, log_hash)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (log_hash) DO NOTHING
            """, (r["nick"], r["lock_type"], r["success"], r["elapsed_time"], r["log_hash"]))
            if cur.rowcount > 0:
                new_records += 1
        except Exception as e:
            print(f"[ERROR] Błąd przy dodawaniu do bazy: {e}")
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Dodano {new_records} nowych wpisów do bazy.")

def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpick_logs", conn)
    conn.close()
    if df.empty:
        print("[DEBUG] Brak danych w bazie.")
        return None

    grouped = df.groupby(["nick", "lock_type"]).agg(
        total_attempts=pd.NamedAgg(column="success", aggfunc="count"),
        successful=pd.NamedAgg(column="success", aggfunc="sum"),
        failed=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        accuracy=pd.NamedAgg(column="success", aggfunc=lambda x: round(100 * x.sum() / len(x), 2)),
        avg_time=pd.NamedAgg(column="elapsed_time", aggfunc=lambda x: round(x.mean(), 2))
    ).reset_index()

    grouped.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    return grouped

def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return

    table_str = tabulate(df, headers="keys", tablefmt="github", showindex=False, stralign="center", numalign="center")
    payload = {"content": f"```\n{table_str}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Wysłano dane na webhook, status: {response.status_code}")

def process_logs():
    print("[DEBUG] Sprawdzanie logów...")
    if not os.path.exists(FTP_LOG_PATH):
        print("[ERROR] Ścieżka do logów nie istnieje:", FTP_LOG_PATH)
        return
    log_files = [f for f in os.listdir(FTP_LOG_PATH) if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików logów.")

    for fname in sorted(log_files):
        full_path = os.path.join(FTP_LOG_PATH, fname)
        try:
            with open(full_path, "r", encoding="utf-16") as f:
                content = f.read()
            print(f"[DEBUG] Odczytano plik: {fname}")
            records = parse_log_content(content)
            insert_to_db(records)
        except Exception as e:
            print(f"[ERROR] Błąd przy przetwarzaniu pliku {fname}: {e}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        try:
            process_logs()
            df = create_dataframe()
            send_to_discord(df)
        except Exception as e:
            print("[ERROR] Błąd główny:", e)
        print("[DEBUG] Oczekiwanie 60s...")
        time.sleep(60)

# --- START ---
if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host='0.0.0.0', port=3000)
