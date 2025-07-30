import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
from flask import Flask
from tabulate import tabulate
import requests
from io import BytesIO
from datetime import datetime

# Konfiguracja
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

# Flask
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# Inicjalizacja bazy
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_logs (
            id SERIAL PRIMARY KEY,
            nickname TEXT,
            steam_id TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            failed_attempts INT,
            lock_type TEXT,
            owner_nickname TEXT,
            owner_steam_id TEXT,
            location TEXT,
            timestamp TIMESTAMP,
            raw_line TEXT UNIQUE
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] Zainicjalizowano bazę danych.")

# Parsowanie logów
def parse_log_content(content):
    print("[DEBUG] Rozpoczynam parsowanie logu...")
    pattern = re.compile(
        r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}: \[LogMinigame] \[LockpickingMinigame_C] "
        r"User: (?P<nickname>.+?) \(\d+, (?P<steam_id>\d+)\)\. "
        r"Success: (?P<success>Yes|No)\. "
        r"Elapsed time: (?P<elapsed_time>\d+\.\d+)\. "
        r"Failed attempts: (?P<failed_attempts>\d+)\. "
        r"Target object: .+?\. "
        r"Lock type: (?P<lock_type>.+?)\. "
        r"User owner: \d+\(\[(?P<owner_steam_id>\d+)] (?P<owner_nickname>.+?)\)\. "
        r"Location: (?P<location>X=[^ ]+ Y=[^ ]+ Z=[^\s]+)"
    )

    entries = []
    for match in pattern.finditer(content):
        data = match.groupdict()
        data["success"] = data["success"] == "Yes"
        data["elapsed_time"] = float(data["elapsed_time"])
        data["failed_attempts"] = int(data["failed_attempts"])
        data["raw_line"] = match.group(0)
        timestamp_match = re.match(r"(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})", data["raw_line"])
        data["timestamp"] = datetime.strptime(timestamp_match.group(1), "%Y.%m.%d-%H.%M.%S") if timestamp_match else None
        entries.append(data)
        print(f"[DEBUG] Rozpoznano wpis: {data}")
    return entries

# Wysyłka do Discorda
def send_to_discord(df):
    df["Ilość wszystkich prób"] = df["udane"] + df["nieudane"]
    df["Skuteczność"] = (df["udane"] / df["Ilość wszystkich prób"] * 100).round(1).astype(str) + "%"
    df["Średni czas"] = df["suma_czasu"] / df["udane"]
    df["Średni czas"] = df["Średni czas"].fillna(0).round(2)

    df = df[["nickname", "lock_type", "Ilość wszystkich prób", "udane", "nieudane", "Skuteczność", "Średni czas"]]
    df.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    tabela = tabulate(df, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    print(f"[DEBUG] Wysyłam tabelę do Discorda:\n{tabela}")
    requests.post(WEBHOOK_URL, json={"content": f"```\n{tabela}\n```"})

# Tworzenie DataFrame
def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM lockpicking_logs", conn)
    conn.close()
    if df.empty:
        print("[DEBUG] Brak danych do przetworzenia.")
        return pd.DataFrame()

    grouped = df.groupby(["nickname", "lock_type"]).agg({
        "success": ["sum", lambda x: (~x).sum()],
        "elapsed_time": "sum"
    }).reset_index()

    grouped.columns = ["nickname", "lock_type", "udane", "nieudane", "suma_czasu"]
    return grouped

# Przetwarzanie logów
def process_logs():
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)

        filenames = []
        ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
        log_files = [name for name in filenames if name.startswith("gameplay_") and name.endswith(".log")]

        print(f"[DEBUG] Znaleziono {len(log_files)} plików logów.")

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        nowe_wpisy = 0
        for filename in log_files:
            print(f"[DEBUG] Przetwarzam: {filename}")
            bio = BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode("utf-16-le", errors="ignore")
            entries = parse_log_content(content)

            for entry in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpicking_logs (
                            nickname, steam_id, success, elapsed_time, failed_attempts,
                            lock_type, owner_nickname, owner_steam_id, location, timestamp, raw_line
                        ) VALUES (%(nickname)s, %(steam_id)s, %(success)s, %(elapsed_time)s, %(failed_attempts)s,
                                  %(lock_type)s, %(owner_nickname)s, %(owner_steam_id)s, %(location)s, %(timestamp)s, %(raw_line)s)
                        ON CONFLICT (raw_line) DO NOTHING
                    """, entry)
                    nowe_wpisy += cur.rowcount
                except Exception as e:
                    print(f"[ERROR] Błąd przy INSERT: {e}")

        conn.commit()
        cur.close()
        conn.close()

        print(f"[DEBUG] Dodano {nowe_wpisy} nowych wpisów.")
        return nowe_wpisy > 0

# Główna pętla
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        try:
            if process_logs():
                df = create_dataframe()
                if not df.empty:
                    send_to_discord(df)
            else:
                print("[DEBUG] Brak nowych wpisów.")
        except Exception as e:
            print(f"[ERROR] Wystąpił błąd: {e}")
        time.sleep(60)

if __name__ == "__main__":
    main_loop()
