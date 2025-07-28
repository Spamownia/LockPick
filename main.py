import os
import time
import re
import io
import pandas as pd
import psycopg2
import requests
from ftplib import FTP_TLS
from datetime import datetime
from tabulate import tabulate
from flask import Flask

# Konfiguracje
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

TABLE_NAME = "lockpick_logs"

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# Połączenie z bazą
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Parsowanie zawartości logu
def parse_log_content(content):
    content = content.decode("utf-16-le", errors="ignore")
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<Nick>.+?) "
        r"picked a (?P<LockType>.+?) lock\. Success: (?P<Success>Yes|No)\. "
        r"Elapsed time: (?P<Time>[0-9.]+)s"
    )
    return pattern.findall(content)

# Tworzenie dataframe z danych
def create_dataframe(entries):
    rows = []
    for nick, lock_type, success, time_str in entries:
        rows.append({
            "Nick": nick.strip(),
            "LockType": lock_type.strip(),
            "Success": success == "Yes",
            "Time": float(time_str)
        })
    return pd.DataFrame(rows)

# Inicjalizacja bazy i tabeli
def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    Nick TEXT,
                    LockType TEXT,
                    Success BOOLEAN,
                    Time FLOAT
                )
            """)
            conn.commit()

# Zapis danych do bazy
def insert_data(df):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(f"""
                    INSERT INTO {TABLE_NAME} (Nick, LockType, Success, Time)
                    VALUES (%s, %s, %s, %s)
                """, (row["Nick"], row["LockType"], row["Success"], row["Time"]))
            conn.commit()

# Wysyłka tabeli na webhook
def send_to_discord():
    with get_db_connection() as conn:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)

    if df.empty:
        print("[DEBUG] Brak danych w bazie.")
        return

    df["Attempts"] = 1
    grouped = df.groupby(["Nick", "LockType"])
    summary = grouped.agg({
        "Attempts": "count",
        "Success": "sum",
        "Time": "mean"
    }).reset_index()

    summary["Failures"] = summary["Attempts"] - summary["Success"]
    summary["SuccessRate"] = (summary["Success"] / summary["Attempts"] * 100).round(1)
    summary["AvgTime"] = summary["Time"].round(2)

    final_df = summary[["Nick", "LockType", "Attempts", "Success", "Failures", "SuccessRate", "AvgTime"]]
    final_df.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    table = tabulate(final_df, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    print("[DEBUG] Wysyłanie tabeli do Discorda:\n", table)

    data = {
        "content": f"```\n{table}\n```"
    }
    requests.post(WEBHOOK_URL, json=data)

# Pobieranie listy plików i zawartości ostatniego pliku
def get_latest_log_file(ftp):
    files = []
    ftp.retrlines("LIST", lambda line: files.append(line))
    gameplay_logs = [f.split()[-1] for f in files if f.endswith(".log") and "gameplay_" in f]
    gameplay_logs.sort(reverse=True)
    return gameplay_logs[0] if gameplay_logs else None

# Główna pętla
def main_loop():
    print("[DEBUG] Start programu")
    init_db()
    last_known_log = ""
    last_known_content = ""

    while True:
        try:
            ftps = FTP_TLS()
            ftps.connect(FTP_HOST, FTP_PORT)
            ftps.login(FTP_USER, FTP_PASS)
            ftps.prot_p()
            ftps.cwd(FTP_DIR)

            latest_file = get_latest_log_file(ftps)

            if not latest_file:
                print("[DEBUG] Brak logów na FTP.")
                ftps.quit()
                time.sleep(60)
                continue

            r = io.BytesIO()
            ftps.retrbinary(f"RETR {latest_file}", r.write)
            ftps.quit()

            current_content = r.getvalue()

            if latest_file != last_known_log or current_content != last_known_content:
                entries = parse_log_content(current_content)
                print(f"[DEBUG] Przetwarzanie: {latest_file} - znaleziono {len(entries)} wpisów")

                if entries:
                    df = create_dataframe(entries)
                    insert_data(df)
                    send_to_discord()

                last_known_log = latest_file
                last_known_content = current_content
            else:
                print("[DEBUG] Brak nowych wpisów w logu.")

        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(60)

if __name__ == "__main__":
    main_loop()
    app.run(host='0.0.0.0', port=3000)
