import os
import time
import re
import io
import pandas as pd
import psycopg2
import requests
from datetime import datetime
from tabulate import tabulate
from flask import Flask
from ftplib import FTP_TLS

# --- CONFIG ---
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
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- FLASK ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"

# --- FUNKCJE ---
def connect_to_ftp():
    print("[FTP] Łączenie z FTP...")
    ftp = FTP_TLS()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.prot_p()
    ftp.cwd(FTP_LOG_DIR)
    print("[FTP] Połączono z FTP.")
    return ftp

def init_db():
    print("[DB] Inicjalizacja połączenia z bazą danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            log_hash TEXT PRIMARY KEY,
            nick TEXT,
            lock_type TEXT,
            success BOOLEAN,
            elapsed_time FLOAT
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("[DB] Baza danych gotowa.")

def generate_hash(entry):
    return hash(entry)

def parse_log_content(content):
    print("[PARSE] Przetwarzanie zawartości logu...")
    pattern = re.compile(
        r"\[LogMinigame] \[LockpickingMinigame_C] User: (?P<nick>.+?) \(\d+, \d+\)\. "
        r"Success: (?P<success>Yes|No)\. Elapsed time: (?P<elapsed>[\d.]+)\. .*?Lock type: (?P<lock>Basic|Medium|Advanced)",
        re.MULTILINE
    )
    entries = []
    for match in pattern.finditer(content):
        nick = match.group("nick")
        success = match.group("success") == "Yes"
        elapsed_time = float(match.group("elapsed"))
        lock_type = match.group("lock")
        entry_text = match.group(0)
        entry_hash = str(generate_hash(entry_text))
        entries.append({
            "hash": entry_hash,
            "nick": nick,
            "success": success,
            "elapsed_time": elapsed_time,
            "lock_type": lock_type
        })
    print(f"[PARSE] Rozpoznano {len(entries)} wpisów.")
    return entries

def fetch_new_entries():
    print("[FTP] Pobieranie nowych logów...")
    ftp = connect_to_ftp()
    filenames = []
    ftp.retrlines('LIST', lambda line: filenames.append(line.split()[-1]))
    log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[FTP] Znaleziono {len(log_files)} plików.")

    all_entries = []
    for file in log_files:
        with io.BytesIO() as f:
            ftp.retrbinary(f"RETR {file}", f.write)
            f.seek(0)
            content = f.read().decode("utf-16-le", errors="ignore")
            entries = parse_log_content(content)
            all_entries.extend(entries)
    ftp.quit()
    print(f"[FTP] Łącznie zebrano {len(all_entries)} wpisów.")
    return all_entries

def store_and_filter_new_entries(entries):
    print("[DB] Zapisywanie i filtrowanie nowych wpisów...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    new_entries = []
    for e in entries:
        cursor.execute("SELECT 1 FROM lockpick_logs WHERE log_hash = %s", (e["hash"],))
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO lockpick_logs (log_hash, nick, lock_type, success, elapsed_time)
                VALUES (%s, %s, %s, %s, %s)
            """, (e["hash"], e["nick"], e["lock_type"], e["success"], e["elapsed_time"]))
            new_entries.append(e)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"[DB] Dodano {len(new_entries)} nowych wpisów.")
    return new_entries

def create_dataframe():
    print("[DB] Tworzenie tabeli podsumowującej...")
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpick_logs", conn)
    conn.close()

    if df.empty:
        print("[DB] Brak danych w tabeli.")
        return None

    summary = (
        df.groupby(["nick", "lock_type"])
        .agg(
            total_tries=pd.NamedAgg(column="success", aggfunc="count"),
            successes=pd.NamedAgg(column="success", aggfunc="sum"),
            fails=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
            avg_time=pd.NamedAgg(column="elapsed_time", aggfunc="mean")
        )
        .reset_index()
    )
    summary["efficiency"] = (summary["successes"] / summary["total_tries"] * 100).round(2)

    summary = summary.rename(columns={
        "nick": "Nick",
        "lock_type": "Zamek",
        "total_tries": "Ilość wszystkich prób",
        "successes": "Udane",
        "fails": "Nieudane",
        "efficiency": "Skuteczność",
        "avg_time": "Średni czas"
    })
    return summary

def send_to_discord(df):
    print("[DISCORD] Wysyłanie danych na webhook...")
    table = tabulate(
        df,
        headers="keys",
        tablefmt="github",
        stralign="center",
        numalign="center"
    )
    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DISCORD] Status wysyłki: {response.status_code}")

def main_loop():
    print("[INFO] Uruchomienie pętli głównej...")
    init_db()
    while True:
        try:
            print(f"[LOOP] Czas: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            entries = fetch_new_entries()
            new_entries = store_and_filter_new_entries(entries)
            if new_entries:
                df = create_dataframe()
                if df is not None:
                    send_to_discord(df)
            else:
                print("[INFO] Brak nowych wpisów. Oczekiwanie...")
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(60)

# --- ENTRYPOINT ---
if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
