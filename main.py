import os
import re
import time
import ftplib
import threading
import requests
import psycopg2
import pandas as pd
from flask import Flask
from tabulate import tabulate
from datetime import datetime

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

PROCESSED_LINES = set()

# --- FUNKCJE ---

def get_ftp_connection():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    return ftp

def list_log_files(ftp):
    files = []
    ftp.retrlines('LIST', lambda line: files.append(line.split()[-1]))
    return [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

def read_log_file(ftp, filename):
    print(f"[DEBUG] Odczyt pliku: {filename}")
    lines = []

    def handle_binary(more_data):
        lines.append(more_data.decode("utf-16le", errors="ignore"))

    ftp.retrbinary(f"RETR {filename}", callback=handle_binary)
    return "".join(lines)

def parse_log_content(content):
    results = []
    pattern = re.compile(r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (.+?) .*?Success: (Yes|No)\. Elapsed time: ([\d.]+)")

    for match in pattern.finditer(content):
        user = match.group(1)
        success = match.group(2)
        time_taken = float(match.group(3))
        lock_type = re.search(r'lockType: (\w+)', match.group(0))
        lock = lock_type.group(1) if lock_type else "Unknown"

        raw_line = match.group(0)
        if raw_line in PROCESSED_LINES:
            continue
        PROCESSED_LINES.add(raw_line)

        results.append({
            "Nick": user,
            "Zamek": lock,
            "Sukces": success == "Yes",
            "Czas": time_taken
        })
    return results

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            zamek TEXT,
            sukces BOOLEAN,
            czas FLOAT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_data_to_db(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for row in data:
        cur.execute(
            "INSERT INTO lockpicking (nick, zamek, sukces, czas) VALUES (%s, %s, %s, %s)",
            (row["Nick"], row["Zamek"], row["Sukces"], row["Czas"])
        )
    conn.commit()
    cur.close()
    conn.close()

def fetch_all_data():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM lockpicking", conn)
    conn.close()
    return df

def create_dataframe(df):
    if df.empty:
        return None

    summary = (
        df.groupby(["nick", "zamek"])
        .agg(
            Ilosc_probow=("sukces", "count"),
            Udane=("sukces", "sum"),
            Nieudane=("sukces", lambda x: (~x).sum()),
            Skutecznosc=("sukces", lambda x: f"{x.mean() * 100:.1f}%"),
            Sredni_czas=("czas", lambda x: f"{x.mean():.2f}s")
        )
        .reset_index()
    )

    summary.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    return summary

def send_to_discord(df):
    table = tabulate(df.values.tolist(), headers=df.columns, tablefmt="grid", stralign="center", numalign="center")
    payload = {
        "content": f"```\n{table}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[INFO] Tabela wysłana na Discord.")
    else:
        print(f"[ERROR] Błąd podczas wysyłania na Discord: {response.status_code}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()

    while True:
        try:
            ftp = get_ftp_connection()
            files = list_log_files(ftp)
            all_results = []

            for file in files:
                content = read_log_file(ftp, file)
                parsed = parse_log_content(content)
                if parsed:
                    print(f"[DEBUG] Nowe wpisy w {file}: {len(parsed)}")
                    all_results.extend(parsed)

            ftp.quit()

            if all_results:
                insert_data_to_db(all_results)
                df = fetch_all_data()
                summary_df = create_dataframe(df)
                if summary_df is not None:
                    send_to_discord(summary_df)
            else:
                print("[DEBUG] Brak nowych wpisów.")
        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(60)

# --- FLASK SERVER ---

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

if __name__ == '__main__':
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
