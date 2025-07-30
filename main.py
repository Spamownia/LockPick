import os
import re
import time
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from ftplib import FTP_TLS
from io import BytesIO, StringIO
from flask import Flask

# Konfiguracja
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

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def connect_ftp():
    print("[DEBUG] Nawiązywanie połączenia FTP...")
    ftps = FTP_TLS()
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.login(FTP_USER, FTP_PASS)
    ftps.prot_p()
    print("[DEBUG] Połączono z FTP.")
    return ftps

def list_log_files(ftps):
    print("[DEBUG] Pobieranie listy plików z FTP...")
    ftps.cwd(LOG_DIR)
    lines = []
    ftps.retrlines('LIST', lines.append)
    filenames = []
    for line in lines:
        parts = line.split()
        if len(parts) >= 9:
            filename = parts[-1]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                filenames.append(filename)
    print(f"[DEBUG] Znaleziono {len(filenames)} plików gameplay_*.log")
    return filenames

def download_log_file(ftps, filename):
    print(f"[DEBUG] Pobieranie pliku: {filename}")
    buffer = BytesIO()
    ftps.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)
    content = buffer.read().decode("utf-16-le", errors="ignore")
    print(f"[DEBUG] Pobrano {len(content)} znaków z pliku {filename}")
    return content

def parse_log_content(content):
    print("[DEBUG] Parsowanie zawartości loga...")
    pattern = re.compile(
        r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}: \[LogMinigame] \[LockpickingMinigame_C] "
        r"User: (?P<nick>[^\(]+) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d\.]+)\. "
        r"Failed attempts: (?P<fail>\d+)\. Target object: (?P<object>[^\(]+)\(ID: \d+\)\. Lock type: (?P<lock>[^\.\n]+)\. "
        r"User owner: \d+\(\[\d+\] .+?\)\. Location: (?P<location>X=[\d\.\-]+ Y=[\d\.\-]+ Z=[\d\.\-]+)"
    )
    matches = pattern.findall(content)
    print(f"[DEBUG] Rozpoznano {len(matches)} wpisów lockpicking")
    return matches

def create_dataframe(entries):
    print("[DEBUG] Tworzenie DataFrame z wpisów...")
    df = pd.DataFrame(entries, columns=["Nick", "Success", "Time", "Failed", "Target", "Lock", "Location"])
    df["Success"] = df["Success"].map({"Yes": 1, "No": 0})
    df["Time"] = df["Time"].astype(float)
    df["Failed"] = df["Failed"].astype(int)
    print("[DEBUG] Utworzono DataFrame.")
    return df

def aggregate_stats(df):
    print("[DEBUG] Agregowanie statystyk...")
    grouped = df.groupby(["Nick", "Lock"]).agg(
        Total_Attempts=("Success", "count"),
        Successes=("Success", "sum"),
        Fails=("Success", lambda x: (x == 0).sum()),
        Effectiveness=("Success", "mean"),
        Avg_Time=("Time", "mean")
    ).reset_index()

    grouped["Effectiveness"] = (grouped["Effectiveness"] * 100).round(2).astype(str) + "%"
    grouped["Avg_Time"] = grouped["Avg_Time"].round(2)
    print("[DEBUG] Statystyki zagregowane.")
    return grouped

def send_to_discord(df):
    print("[DEBUG] Wysyłanie tabeli do Discorda...")
    table = tabulate(
        df,
        headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"],
        tablefmt="grid",
        stralign="center",
        numalign="center"
    )
    payload = {
        "content": f"```\n{table}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Wysłano dane do Discorda, status: {response.status_code}")

def init_db():
    print("[DEBUG] Inicjalizacja połączenia z bazą danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking (
                nick TEXT,
                lock TEXT,
                success INTEGER,
                time FLOAT,
                failed INTEGER,
                target TEXT,
                location TEXT
            );
        """)
        conn.commit()
    conn.close()
    print("[DEBUG] Baza danych gotowa.")

def save_to_db(df):
    print(f"[DEBUG] Zapis {len(df)} wpisów do bazy danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO lockpicking (nick, lock, success, time, failed, target, location)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, tuple(row))
        conn.commit()
    conn.close()
    print("[DEBUG] Dane zapisane do bazy.")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    last_results = None

    while True:
        try:
            ftps = connect_ftp()
            log_files = list_log_files(ftps)
            all_entries = []

            for filename in log_files:
                content = download_log_file(ftps, filename)
                entries = parse_log_content(content)
                all_entries.extend(entries)

            ftps.quit()

            if not all_entries:
                print("[DEBUG] Brak nowych danych do przetworzenia.")
                time.sleep(60)
                continue

            df = create_dataframe(all_entries)
            save_to_db(df)
            summary = aggregate_stats(df)

            if not summary.equals(last_results):
                send_to_discord(summary)
                last_results = summary.copy()
            else:
                print("[DEBUG] Brak zmian w statystykach - nie wysyłam na webhook.")
        except Exception as e:
            print(f"[ERROR] Wystąpił błąd: {e}")

        time.sleep(60)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
