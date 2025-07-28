import re
import io
import ssl
import time
import ftplib
import psycopg2
import requests
from collections import defaultdict
from contextlib import closing
from datetime import datetime
from tabulate import tabulate

# --- KONFIGURACJA ---
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

# --- INICJALIZACJA BAZY ---
def initialize_database():
    with closing(psycopg2.connect(**DB_CONFIG)) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_stats (
                    nick TEXT,
                    lock_type TEXT,
                    success BOOLEAN,
                    time FLOAT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_logs (
                    filename TEXT PRIMARY KEY
                );
            """)
        conn.commit()

# --- POŁĄCZENIE FTP ---
def list_log_files():
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(LOG_DIR)
        files = []
        ftp.retrlines("LIST", lambda line: files.append(line))
        filenames = [line.split()[-1] for line in files if "gameplay_" in line and line.endswith(".log")]
        return filenames

def download_file(filename):
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(LOG_DIR)
        r = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", r.write)
        return r.getvalue().decode('utf-16le', errors='ignore')

# --- PARSOWANIE ---
def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (.*?) \([^\)]+\)\. Success: (Yes|No)\. Elapsed time: ([\d.]+)\. .*?Lock type: (\w+)\.",
        re.MULTILINE
    )
    results = []
    for match in pattern.finditer(content):
        nick, success, elapsed, lock_type = match.groups()
        results.append({
            "nick": nick.strip(),
            "success": success == "Yes",
            "time": float(elapsed),
            "lock_type": lock_type.strip()
        })
    return results

# --- ZAPIS DO BAZY ---
def insert_log_data(filename, entries):
    if not entries:
        return
    with closing(psycopg2.connect(**DB_CONFIG)) as conn:
        with conn.cursor() as cur:
            for entry in entries:
                cur.execute("""
                    INSERT INTO lockpick_stats (nick, lock_type, success, time)
                    VALUES (%s, %s, %s, %s)
                """, (entry["nick"], entry["lock_type"], entry["success"], entry["time"]))
            cur.execute("""
                INSERT INTO processed_logs (filename)
                VALUES (%s)
                ON CONFLICT DO NOTHING
            """, (filename,))
        conn.commit()

def already_processed(filename):
    with closing(psycopg2.connect(**DB_CONFIG)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM processed_logs WHERE filename = %s", (filename,))
            return cur.fetchone() is not None

# --- GENEROWANIE TABELI ---
def generate_summary_table():
    with closing(psycopg2.connect(**DB_CONFIG)) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, lock_type, COUNT(*) AS total, 
                       COUNT(*) FILTER (WHERE success) AS success_count,
                       COUNT(*) FILTER (WHERE NOT success) AS fail_count,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE success) / COUNT(*), 2) AS accuracy,
                       ROUND(AVG(time), 2) AS avg_time
                FROM lockpick_stats
                GROUP BY nick, lock_type
                ORDER BY nick, lock_type
            """)
            rows = cur.fetchall()

    if not rows:
        print("[INFO] Brak danych do wyświetlenia.")
        return None

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność (%)", "Średni czas (s)"]
    table = tabulate(rows, headers=headers, tablefmt="github", stralign="center", numalign="center")
    print("\n=== Statystyki Lockpicków ===\n")
    print(table)
    return table

# --- WYSYŁKA NA WEBHOOK ---
def send_to_webhook(table_text):
    if not table_text:
        return
    payload = {
        "content": f"```\n{table_text}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code != 204:
        print(f"[ERROR] Błąd wysyłania na webhook: {response.status_code}")
    else:
        print("[INFO] Tabela wysłana na Discord webhook.")

# --- GŁÓWNA PĘTLA ---
def main():
    print("[DEBUG] Start main()")
    initialize_database()
    files = list_log_files()
    print(f"[DEBUG] Znaleziono {len(files)} plików.")

    for filename in files:
        if already_processed(filename):
            print(f"[DEBUG] Pomijam przetworzony plik: {filename}")
            continue
        print(f"[INFO] Przetwarzanie pliku: {filename}")
        content = download_file(filename)
        entries = parse_log_content(content)
        insert_log_data(filename, entries)
        print(f"[INFO] Zapisano {len(entries)} wpisów z pliku: {filename}")

    summary_table = generate_summary_table()
    send_to_webhook(summary_table)

if __name__ == "__main__":
    main()
