import os
import re
import io
import ftplib
import psycopg2
import pandas as pd
from datetime import datetime
from tabulate import tabulate
import requests

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOGS_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- FUNKCJE ---

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_stats (
                id SERIAL PRIMARY KEY,
                nickname TEXT,
                lock_type TEXT,
                difficulty TEXT,
                success BOOLEAN,
                duration REAL,
                timestamp TIMESTAMP,
                log_file TEXT
            )
        """)
        conn.commit()
        print("[DEBUG] Tabela lockpicking_stats sprawdzona/utworzona.")

def parse_log_content(content, log_file):
    entries = []
    lines = content.splitlines()
    for line in lines:
        match = re.search(
            r"User:\s*(.*?)\s*\|\s*Lock:\s*(.*?)\s*\|\s*Difficulty:\s*(.*?)\s*\|\s*Success:\s*(Yes|No)\.?\s*\|\s*Elapsed time:\s*([0-9.]+)\s*seconds",
            line
        )
        if match:
            nickname, lock_type, difficulty, success_str, duration = match.groups()
            success = success_str == "Yes"
            timestamp = extract_timestamp_from_line(line)
            entries.append({
                "nickname": nickname.strip(),
                "lock_type": lock_type.strip(),
                "difficulty": difficulty.strip(),
                "success": success,
                "duration": float(duration),
                "timestamp": timestamp,
                "log_file": log_file
            })
    print(f"[DEBUG] Przetworzono {len(entries)} wpisów z {log_file}")
    return entries

def extract_timestamp_from_line(line):
    timestamp_match = re.match(r"(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})", line)
    if timestamp_match:
        try:
            return datetime.strptime(timestamp_match.group(1), "%Y.%m.%d-%H.%M.%S")
        except ValueError:
            return None
    return None

def insert_entries_to_db(conn, entries):
    with conn.cursor() as cur:
        for entry in entries:
            cur.execute("""
                INSERT INTO lockpicking_stats (nickname, lock_type, difficulty, success, duration, timestamp, log_file)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                entry["nickname"],
                entry["lock_type"],
                entry["difficulty"],
                entry["success"],
                entry["duration"],
                entry["timestamp"],
                entry["log_file"]
            ))
        conn.commit()
        print(f"[DEBUG] Wstawiono {len(entries)} wpisów do bazy danych.")

def fetch_all_log_files():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOGS_DIR)
    print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")
    files = ftp.nlst()
    log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono plików: {len(log_files)}")
    return ftp, log_files

def read_log_file(ftp, filename):
    buffer = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)
    content = buffer.read().decode("utf-16-le", errors="ignore")
    return content

def generate_summary_table(conn):
    df = pd.read_sql_query("SELECT * FROM lockpicking_stats", conn)

    if df.empty:
        return "[INFO] Brak danych w bazie."

    grouped = df.groupby(["nickname", "lock_type"]).agg(
        total_attempts=pd.NamedAgg(column="success", aggfunc="count"),
        successful=pd.NamedAgg(column="success", aggfunc="sum"),
        failed=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        success_rate=pd.NamedAgg(column="success", aggfunc=lambda x: round(100 * x.mean(), 1)),
        avg_duration=pd.NamedAgg(column="duration", aggfunc=lambda x: round(x.mean(), 2)),
    ).reset_index()

    grouped.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table_str = tabulate(grouped, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    return f"```{table_str}```"

def send_to_webhook(table_message):
    data = {"content": table_message}
    response = requests.post(WEBHOOK_URL, json=data)
    if response.ok:
        print("[OK] Wysłano dane na webhook.")
    else:
        print(f"[ERROR] Błąd wysyłania na webhook: {response.status_code}")

# --- MAIN ---

def main():
    print("[DEBUG] Start programu")
    conn = connect_db()
    create_table_if_not_exists(conn)

    ftp, log_files = fetch_all_log_files()

    for filename in log_files:
        try:
            content = read_log_file(ftp, filename)
            entries = parse_log_content(content, filename)
            if entries:
                insert_entries_to_db(conn, entries)
        except Exception as e:
            print(f"[ERROR] Błąd podczas przetwarzania {filename}: {e}")

    summary_table = generate_summary_table(conn)
    print(summary_table)
    send_to_webhook(summary_table)
    conn.close()
    ftp.quit()

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    main()
