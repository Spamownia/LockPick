import os
import re
import io
import time
import ftplib
import psycopg2
import pandas as pd
import requests
from tabulate import tabulate
from flask import Flask

# --- Konfiguracja FTP ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOGS_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# --- Webhook Discord ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Konfiguracja DB ---
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- Parsowanie treści logów ---
def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame\]\s+\[LockpickingMinigame_C\]\s+User:\s+(?P<user>.+?)\s+Lock:\s+(?P<lock>[\w\s]+)\s+\[Success:\s+(?P<success>Yes|No)\]\.\s+Elapsed time:\s+(?P<time>[0-9.]+)s"
    )

    entries = []
    for line in content.splitlines():
        match = pattern.search(line)
        if match:
            entries.append({
                "Nick": match.group("user").strip(),
                "LockType": match.group("lock").strip(),
                "Success": match.group("success") == "Yes",
                "Time": float(match.group("time"))
            })

    return entries

# --- Połączenie z bazą danych ---
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

# --- Tworzenie tabeli w bazie danych ---
def init_db():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicking_log (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    locktype TEXT,
                    success BOOLEAN,
                    time FLOAT
                );
            """)
        conn.commit()
    print("[INFO] Tabela 'lockpicking_log' gotowa.")

# --- Wstawianie danych do bazy ---
def insert_entries(entries):
    with connect_db() as conn:
        with conn.cursor() as cur:
            for e in entries:
                cur.execute("""
                    INSERT INTO lockpicking_log (nick, locktype, success, time)
                    VALUES (%s, %s, %s, %s);
                """, (e["Nick"], e["LockType"], e["Success"], e["Time"]))
        conn.commit()
    print(f"[INFO] Zapisano {len(entries)} wpisów do bazy.")

# --- Pobieranie logów z FTP ---
def download_all_log_files():
    log_files = []
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOGS_PATH)
            files = []
            ftp.retrlines("LIST", lambda line: files.append(line))

            for entry in files:
                parts = entry.split()
                name = parts[-1]
                if name.startswith("gameplay_") and name.endswith(".log"):
                    with io.BytesIO() as f:
                        ftp.retrbinary(f"RETR {name}", f.write)
                        f.seek(0)
                        content = f.read().decode("utf-16-le")
                        log_files.append(content)
                        print(f"[FTP] ✓ {name}")
    except Exception as e:
        print(f"[ERROR] FTP error: {e}")
    return log_files

# --- Tworzenie DataFrame i agregacja ---
def create_dataframe():
    with connect_db() as conn:
        df = pd.read_sql_query("SELECT * FROM lockpicking_log;", conn)

    if df.empty:
        print("[INFO] Brak danych do analizy.")
        return None

    df["Attempts"] = 1
    grouped = df.groupby(["nick", "locktype"]).agg(
        Attempts=("Attempts", "sum"),
        Successes=("success", "sum"),
        Failures=("success", lambda x: (~x).sum()),
        Accuracy=("success", lambda x: round(100 * x.sum() / len(x), 1)),
        AvgTime=("time", lambda x: round(x.mean(), 2))
    ).reset_index()

    grouped.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    return grouped

# --- Wysyłka do Discorda ---
def send_to_discord(df):
    if df is None or df.empty:
        print("[INFO] Brak danych do wysyłki.")
        return

    table_str = tabulate(df, headers="keys", tablefmt="grid", stralign="center", numalign="center")

    payload = {
        "content": f"📊 **Statystyki wytrychowania**\n```{table_str}```"
    }

    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print("[WEBHOOK] ✓ Wysłano dane na Discorda.")
    except Exception as e:
        print(f"[WEBHOOK ERROR] {e}")

# --- Flask dla pingowania ---
app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

# --- Główna pętla przetwarzania ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()

    all_entries = []
    log_contents = download_all_log_files()

    for content in log_contents:
        parsed = parse_log_content(content)
        if parsed:
            all_entries.extend(parsed)

    if all_entries:
        insert_entries(all_entries)
        df = create_dataframe()
        send_to_discord(df)
    else:
        print("[INFO] Brak nowych poprawnych wpisów.")

if __name__ == "__main__":
    from threading import Thread
    t = Thread(target=main_loop)
    t.start()
    app.run(host="0.0.0.0", port=3000)
