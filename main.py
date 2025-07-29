import os
import time
import re
import pandas as pd
import psycopg2
import requests
from io import BytesIO
from ftplib import FTP
from tabulate import tabulate
from flask import Flask
from datetime import datetime

# === KONFIGURACJA ===
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

# === INICJALIZACJA ===
app = Flask(__name__)
last_known_lines = {}

@app.route('/')
def index():
    return "Alive"

def connect_to_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def list_log_files(ftp):
    ftp.cwd(FTP_LOG_DIR)
    files = []
    ftp.retrlines('LIST', lambda line: files.append(line.split()[-1]))
    return [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

def download_file(ftp, filename):
    buffer = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)
    return buffer.read().decode("utf-16le", errors="ignore")

def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame\].+?User:\s*(?P<user>\w+).*?Lock:\s*(?P<lock>\w+).*?Success:\s*(?P<success>Yes|No).*?Elapsed time:\s*(?P<time>[\d.]+)",
        re.IGNORECASE
    )
    entries = []
    for match in pattern.finditer(content):
        entries.append({
            "Nick": match.group("user"),
            "Zamek": match.group("lock"),
            "Sukces": match.group("success") == "Yes",
            "Czas": float(match.group("time"))
        })
    print(f"[DEBUG] Znaleziono wpisów: {len(entries)}")
    return entries

def initialize_database():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicking (
                    nick TEXT,
                    zamek TEXT,
                    sukces BOOLEAN,
                    czas REAL,
                    PRIMARY KEY (nick, zamek, sukces, czas)
                );
            """)
        conn.commit()
    print("[DEBUG] Baza danych zainicjalizowana.")

def insert_new_entries(entries):
    inserted = 0
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for e in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpicking (nick, zamek, sukces, czas)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (e["Nick"], e["Zamek"], e["Sukces"], e["Czas"]))
                    inserted += cur.rowcount
                except Exception as ex:
                    print(f"[ERROR] Insert error: {ex}")
        conn.commit()
    print(f"[DEBUG] Nowe wpisy zapisane: {inserted}")
    return inserted > 0

def create_dataframe():
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql_query("SELECT * FROM lockpicking", conn)
    if df.empty:
        print("[DEBUG] Brak danych w bazie.")
        return None

    grouped = df.groupby(["Nick", "Zamek"])
    summary = grouped.agg(
        Proby=('Sukces', 'count'),
        Udane=('Sukces', 'sum'),
        Nieudane=('Sukces', lambda x: (~x).sum()),
        Skutecznosc=('Sukces', lambda x: f"{round(x.mean()*100)}%"),
        Sredni_czas=('Czas', lambda x: round(x.mean(), 2))
    ).reset_index()

    print(f"[DEBUG] Gotowa tabela z {len(summary)} wierszami.")
    return summary

def send_to_discord(df):
    table = tabulate(
        df.values,
        headers=["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"],
        tablefmt="github",
        colalign=("center",) * 7
    )
    print("[DEBUG] Wysyłanie tabeli na webhook...")
    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Webhook status: {response.status_code}")

def main_loop():
    print("[DEBUG] Start main_loop")
    initialize_database()

    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.utcnow().isoformat()} ---")
        try:
            ftp = connect_to_ftp()
            files = list_log_files(ftp)
            new_entries = []

            for fname in files:
                content = download_file(ftp, fname)
                lines = content.splitlines()
                prev_len = last_known_lines.get(fname, 0)
                new_lines = lines[prev_len:]
                last_known_lines[fname] = len(lines)

                if not new_lines:
                    continue

                parsed = parse_log_content("\n".join(new_lines))
                new_entries.extend(parsed)

            ftp.quit()

            if new_entries:
                print(f"[DEBUG] Nowe zdarzenia: {len(new_entries)}")
                if insert_new_entries(new_entries):
                    df = create_dataframe()
                    if df is not None:
                        send_to_discord(df)
                else:
                    print("[DEBUG] Brak nowych unikalnych wpisów.")
            else:
                print("[DEBUG] Brak nowych zdarzeń.")

        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(60)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
