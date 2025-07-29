import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
import datetime
import requests
from io import StringIO
from flask import Flask
from tabulate import tabulate

# --- Konfiguracja ---
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

CHECK_INTERVAL = 60  # seconds
PROCESSED_FILES = set()

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_stats (
                    nick TEXT,
                    lock_type TEXT,
                    success BOOLEAN,
                    elapsed_time FLOAT,
                    timestamp TIMESTAMPTZ DEFAULT now()
                )
            """)
            conn.commit()

def get_ftp_file_list():
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        files = []
        ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
        return [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

def download_ftp_file(filename):
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        contents = []
        ftp.retrbinary(f"RETR {filename}", contents.append)
        return b"".join(contents).decode("utf-16le", errors="ignore")

def parse_log_content(content):
    pattern = re.compile(
        r"User:\s+(?P<nick>.+?)\s+.*?Type:\s+(?P<lock_type>\w+).*?Success:\s+(?P<success>Yes|No).*?Elapsed time:\s+(?P<time>\d+\.\d+)",
        re.DOTALL
    )
    return [
        {
            "nick": match.group("nick").strip(),
            "lock_type": match.group("lock_type").strip(),
            "success": match.group("success") == "Yes",
            "elapsed_time": float(match.group("time"))
        }
        for match in pattern.finditer(content)
    ]

def insert_data(entries):
    if not entries:
        return
    with connect_db() as conn:
        with conn.cursor() as cur:
            for entry in entries:
                cur.execute("""
                    INSERT INTO lockpick_stats (nick, lock_type, success, elapsed_time)
                    VALUES (%s, %s, %s, %s)
                """, (entry["nick"], entry["lock_type"], entry["success"], entry["elapsed_time"]))
            conn.commit()

def create_dataframe():
    with connect_db() as conn:
        df = pd.read_sql_query("SELECT * FROM lockpick_stats", conn)

    if df.empty:
        return None

    summary = (
        df.groupby(["nick", "lock_type"])
        .agg(
            attempts=pd.NamedAgg(column="success", aggfunc="count"),
            success=pd.NamedAgg(column="success", aggfunc="sum"),
            fail=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
            effectiveness=pd.NamedAgg(column="success", aggfunc=lambda x: f"{100 * x.mean():.1f}%"),
            avg_time=pd.NamedAgg(column="elapsed_time", aggfunc=lambda x: f"{x.mean():.2f}s")
        )
        .reset_index()
    )

    return summary

def format_table(df):
    if df is None or df.empty:
        return "Brak danych do wyświetlenia."

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    rows = df.values.tolist()
    return "```" + tabulate(rows, headers=headers, tablefmt="grid", stralign="center") + "```"

def send_to_discord(message):
    requests.post(WEBHOOK_URL, json={"content": message})

# --- Funkcje debugujące ---
def has_new_entries(entries, conn):
    if not entries:
        return False
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lockpick_stats")
        old_count = cur.fetchone()[0]
        new_count = old_count + len(entries)
        return new_count > old_count

def debug_log_diff(entries):
    print(f"[DEBUG] Liczba znalezionych wpisów: {len(entries)}")
    for e in entries:
        print(f"[DEBUG] → {e}")

def debug_table(df):
    print("[DEBUG] Aktualna tabela:")
    print(format_table(df))

# --- Główna pętla ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.datetime.now(datetime.timezone.utc).isoformat()} ---")
        try:
            filenames = get_ftp_file_list()
            new_files = [f for f in filenames if f not in PROCESSED_FILES]
            print(f"[DEBUG] Nowe pliki do analizy: {new_files}")
            new_entries = []
            for filename in new_files:
                content = download_ftp_file(filename)
                entries = parse_log_content(content)
                debug_log_diff(entries)
                new_entries.extend(entries)
                PROCESSED_FILES.add(filename)

            if new_entries:
                with connect_db() as conn:
                    insert_data(new_entries)
                    df = create_dataframe()
                    debug_table(df)
                    send_to_discord(format_table(df))
            else:
                print("[DEBUG] Brak nowych wpisów w logach.")

        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
