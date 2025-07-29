import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
from io import BytesIO
from datetime import datetime, timezone
from tabulate import tabulate
from flask import Flask
import requests

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOGS_PATH = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require",
}

# --- FLASK ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- FUNKCJE ---
def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOGS_PATH)
    return ftp

def fetch_log_files(ftp):
    files = []
    ftp.retrlines('LIST', lambda line: files.append(line))
    log_filenames = [line.split()[-1] for line in files if line.split()[-1].startswith("gameplay_") and line.endswith(".log")]
    return log_filenames

def download_file(ftp, filename):
    buffer = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)
    return buffer.read().decode("utf-16le", errors="ignore")

def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (.+?) "
        r"used (.+?) lockpick on (.+?) lock: (.+?)\. Success: (Yes|No)\. "
        r"Elapsed time: ([\d.]+)s"
    )
    entries = []
    for match in pattern.finditer(content):
        user, lockpick, lock_type, target, success, elapsed = match.groups()
        entries.append({
            "Nick": user,
            "Zamek": lock_type,
            "Udane": 1 if success == "Yes" else 0,
            "Nieudane": 0 if success == "Yes" else 1,
            "Czas": float(elapsed),
            "raw": match.group(0),
        })
    return entries

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicking_logs (
                    id SERIAL PRIMARY KEY,
                    raw TEXT UNIQUE,
                    nick TEXT,
                    zamek TEXT,
                    success BOOLEAN,
                    czas FLOAT,
                    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
    conn.close()

def insert_new_entries(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    new_rows = 0
    with conn:
        with conn.cursor() as cur:
            for e in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpicking_logs (raw, nick, zamek, success, czas)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (raw) DO NOTHING;
                    """, (e["raw"], e["Nick"], e["Zamek"], e["Udane"] == 1, e["Czas"]))
                    new_rows += cur.rowcount
                except Exception as ex:
                    print("[ERROR] Insert error:", ex)
    conn.close()
    return new_rows

def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT nick, zamek, success, czas FROM lockpicking_logs", conn)
    conn.close()

    if df.empty:
        return None

    df["Udane"] = df["success"].astype(int)
    df["Nieudane"] = (~df["success"]).astype(int)
    df["Próby"] = 1

    grouped = df.groupby(["nick", "zamek"]).agg({
        "Próby": "sum",
        "Udane": "sum",
        "Nieudane": "sum",
        "czas": "mean"
    }).reset_index()

    grouped["Skuteczność"] = (grouped["Udane"] / grouped["Próby"] * 100).round(1).astype(str) + "%"
    grouped["Średni czas"] = grouped["czas"].round(2).astype(str) + "s"

    final_df = grouped[["nick", "zamek", "Próby", "Udane", "Nieudane", "Skuteczność", "Średni czas"]]
    final_df.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    return final_df

def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return

    table_str = tabulate(df.values.tolist(), headers=df.columns, tablefmt="github", stralign="center")
    payload = {
        "content": f"**Statystyki wytrychów:**\n```{table_str}```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    print("[DEBUG] Wysłano dane na Discord:", response.status_code)

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.now(timezone.utc).isoformat()} ---")
        try:
            ftp = connect_ftp()
            files = fetch_log_files(ftp)
            print(f"[DEBUG] Znaleziono {len(files)} plików logów.")
            all_entries = []
            for fname in files:
                content = download_file(ftp, fname)
                entries = parse_log_content(content)
                print(f"[DEBUG] {fname} => {len(entries)} wpisów LockpickingMinigame.")
                all_entries.extend(entries)
            ftp.quit()

            if not all_entries:
                print("[DEBUG] Brak wpisów do analizy.")
            else:
                new_rows = insert_new_entries(all_entries)
                print(f"[DEBUG] Dodano {new_rows} nowych wpisów do bazy.")
                if new_rows > 0:
                    df = create_dataframe()
                    send_to_discord(df)
                else:
                    print("[DEBUG] Brak nowych wpisów – wysyłka pominięta.")

        except Exception as e:
            print("[ERROR]", e)

        time.sleep(60)

if __name__ == "__main__":
    import threading
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
