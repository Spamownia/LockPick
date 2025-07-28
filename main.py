import os
import re
import time
import psycopg2
import requests
from io import BytesIO
from ftplib import FTP
from collections import defaultdict

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

# === REGEXP ===
ENTRY_REGEX = re.compile(
    r"User: (?P<nick>.*?) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<elapsed>\d+\.\d+)\. "
    r"Failed attempts: (?P<failed>\d+)\. .*? Lock type: (?P<lock_type>\w+)\."
)

# === FUNKCJE BAZODANOWE ===
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def initialize_db():
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_stats (
                nick TEXT,
                lock_type TEXT,
                success BOOLEAN,
                elapsed_time REAL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_logs (
                filename TEXT PRIMARY KEY
            );
        """)
        conn.commit()

def save_entries_to_db(entries):
    with get_db_connection() as conn, conn.cursor() as cur:
        for entry in entries:
            cur.execute("""
                INSERT INTO lockpicking_stats (nick, lock_type, success, elapsed_time)
                VALUES (%s, %s, %s, %s);
            """, (entry["nick"], entry["lock_type"], entry["success"], entry["elapsed_time"]))
        conn.commit()

def mark_log_as_processed(filename):
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO processed_logs (filename) VALUES (%s) ON CONFLICT DO NOTHING;", (filename,))
        conn.commit()

def get_processed_logs():
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT filename FROM processed_logs;")
        return set(row[0] for row in cur.fetchall())

def aggregate_stats():
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT nick, lock_type,
                COUNT(*) AS attempts,
                COUNT(*) FILTER (WHERE success) AS successes,
                COUNT(*) FILTER (WHERE NOT success) AS failures,
                ROUND(100.0 * COUNT(*) FILTER (WHERE success) / COUNT(*), 2) AS accuracy,
                ROUND(AVG(elapsed_time), 2) AS avg_time
            FROM lockpicking_stats
            GROUP BY nick, lock_type
            ORDER BY nick, lock_type;
        """)
        return cur.fetchall()

# === FTP I LOGIKA ===
def list_log_files(ftp):
    file_list = []
    ftp.cwd(FTP_LOG_DIR)
    ftp.retrlines("LIST", lambda line: file_list.append(line.split()[-1]))
    return [f for f in file_list if f.startswith("gameplay_") and f.endswith(".log")]

def download_file(ftp, filename):
    bio = BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    return bio.getvalue().decode("utf-16le", errors="ignore")

def parse_log_content(content):
    entries = []
    for match in ENTRY_REGEX.finditer(content):
        entries.append({
            "nick": match.group("nick"),
            "lock_type": match.group("lock_type"),
            "success": match.group("success") == "Yes",
            "elapsed_time": float(match.group("elapsed"))
        })
    return entries

def format_table(rows):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność (%)", "Średni czas"]
    columns = list(zip(*rows)) if rows else [[] for _ in headers]
    col_widths = [max(len(str(item)) for item in [header] + list(col)) for header, col in zip(headers, columns)]

    def fmt_row(row):
        return " | ".join(str(val).center(width) for val, width in zip(row, col_widths))

    line = "-+-".join("-" * width for width in col_widths)
    return "\n".join([fmt_row(headers), line] + [fmt_row(row) for row in rows])

def send_to_webhook(table_text):
    payload = {
        "content": f"```\n{table_text}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[WEBHOOK] Status: {response.status_code}")

# === GŁÓWNA PĘTLA ===
def main_loop():
    print("[DEBUG] Start main_loop")
    initialize_db()
    while True:
        try:
            print("[DEBUG] Łączenie z FTP...")
            ftp = FTP()
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)

            processed = get_processed_logs()
            files = list_log_files(ftp)
            new_files = [f for f in files if f not in processed]

            print(f"[DEBUG] Nowe pliki do przetworzenia: {new_files}")
            all_new_entries = []
            for filename in new_files:
                try:
                    print(f"[DEBUG] Pobieranie i parsowanie: {filename}")
                    content = download_file(ftp, filename)
                    entries = parse_log_content(content)
                    if entries:
                        save_entries_to_db(entries)
                        all_new_entries.extend(entries)
                        mark_log_as_processed(filename)
                        print(f"[OK] Przetworzono {len(entries)} wpisów z {filename}")
                    else:
                        print(f"[INFO] Brak pasujących danych w {filename}")
                except Exception as e:
                    print(f"[ERROR] Błąd przetwarzania {filename}: {e}")

            ftp.quit()

            if all_new_entries:
                print("[DEBUG] Agregowanie i wysyłanie statystyk...")
                stats = aggregate_stats()
                if stats:
                    table = format_table(stats)
                    send_to_webhook(table)
            else:
                print("[INFO] Brak nowych wpisów.")

        except Exception as e:
            print(f"[FATAL] Błąd główny: {e}")

        time.sleep(60)

if __name__ == "__main__":
    main_loop()
