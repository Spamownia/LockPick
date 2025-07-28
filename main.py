import os
import ftplib
import psycopg2
import pandas as pd
import re
import io
import time
import requests
from tabulate import tabulate
from flask import Flask

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# Konfiguracja bazy danych Neon (PostgreSQL)
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Inicjalizacja aplikacji Flask
app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)
    return ftp

def list_log_files(ftp):
    files = []
    ftp.retrlines('LIST', lambda line: files.append(line))
    log_files = [line.split()[-1] for line in files if line.split()[-1].startswith("gameplay_") and line.endswith(".log")]
    return log_files

def download_log_file(ftp, filename):
    with io.BytesIO() as bio:
        ftp.retrbinary(f"RETR {filename}", bio.write)
        bio.seek(0)
        return bio.read().decode("utf-16-le", errors="ignore")

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_logs (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    steamid TEXT,
                    success BOOLEAN,
                    time FLOAT,
                    lock_type TEXT
                )
            """)
            conn.commit()

def parse_log_content(content):
    pattern = re.compile(
        r"""
        ^\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\s+\[LogMinigame\]\s+\[LockpickingMinigame_C\]
        \s+User:\s(?P<nick>.*?)\s\(\d+,\s(?P<steamid>\d+)\)\.
        \s+Success:\s(?P<success>Yes|No)\.
        \s+Elapsed\stime:\s(?P<time>\d+\.\d+)\.
        .*?
        Lock\stype:\s(?P<lock_type>\w+)\.
        """,
        re.VERBOSE | re.MULTILINE
    )
    entries = []
    for match in pattern.finditer(content):
        data = match.groupdict()
        entries.append({
            "nick": data["nick"],
            "steamid": data["steamid"],
            "success": data["success"] == "Yes",
            "time": float(data["time"]),
            "lock_type": data["lock_type"]
        })
    return entries

def insert_entries(entries):
    with connect_db() as conn:
        with conn.cursor() as cur:
            for e in entries:
                cur.execute("""
                    INSERT INTO lockpick_logs (nick, steamid, success, time, lock_type)
                    VALUES (%s, %s, %s, %s, %s)
                """, (e["nick"], e["steamid"], e["success"], e["time"], e["lock_type"]))
            conn.commit()

def summarize_data():
    with connect_db() as conn:
        df = pd.read_sql_query("SELECT * FROM lockpick_logs", conn)

    if df.empty:
        return None

    df["udane"] = df["success"].astype(int)
    df["nieudane"] = (~df["success"]).astype(int)

    grouped = df.groupby(["nick", "lock_type"]).agg({
        "success": "sum",
        "time": "mean",
        "udane": "sum",
        "nieudane": "sum"
    }).reset_index()

    grouped["total"] = grouped["udane"] + grouped["nieudane"]
    grouped["skutecznosc"] = (grouped["udane"] / grouped["total"] * 100).round(2)
    grouped["sredni_czas"] = grouped["time"].round(2)

    grouped = grouped[["nick", "lock_type", "total", "udane", "nieudane", "skutecznosc", "sredni_czas"]]
    grouped.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    return grouped

def send_to_discord(df):
    if df is None or df.empty:
        print("[INFO] Brak danych do wysłania.")
        return

    table = tabulate(df, headers="keys", tablefmt="github", stralign="center", numalign="center")
    print("[DEBUG] Tabela do wysłania:\n", table)

    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)

    if response.status_code == 204:
        print("[OK] Tabela wysłana na Discord webhook.")
    else:
        print(f"[ERROR] Błąd wysyłania webhooka: {response.status_code}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()

    try:
        ftp = connect_ftp()
        print("[OK] Połączono z FTP")

        files = list_log_files(ftp)
        print(f"[DEBUG] Znaleziono {len(files)} plików.")

        all_entries = []

        for fname in files:
            print(f"[INFO] Przetwarzanie pliku: {fname}")
            content = download_log_file(ftp, fname)
            entries = parse_log_content(content)
            print(f"[DEBUG] {len(entries)} wpisów w pliku {fname}")
            all_entries.extend(entries)

        print(f"[DEBUG] Wszystkich wpisów: {len(all_entries)}")

        if all_entries:
            insert_entries(all_entries)
            df = summarize_data()
            send_to_discord(df)
        else:
            print("[INFO] Brak nowych wpisów do analizy.")

    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    main_loop()
    app.run(host="0.0.0.0", port=3000)
