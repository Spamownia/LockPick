import ftplib
import io
import os
import re
import psycopg2
import requests
from datetime import datetime
from collections import defaultdict

# --- KONFIGURACJE ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOGS_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- INICJALIZACJA BAZY ---
def init_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_stats (
                    nick TEXT,
                    lock_type TEXT,
                    success BOOLEAN,
                    elapsed_seconds INTEGER
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_logs (
                    filename TEXT PRIMARY KEY
                )
            """)
        conn.commit()

# --- PARSOWANIE LOGU ---
def parse_log_content(content):
    pattern = re.compile(
        r"User:\s*(?P<nick>.*?)\s*\|\s*Lock type:\s*(?P<lock_type>.*?)\s*\|\s*Success:\s*(?P<success>Yes|No)\s*\|\s*Elapsed time:\s*(?P<minutes>\d+):(?P<seconds>\d+)"
    )
    entries = []
    for match in pattern.finditer(content):
        nick = match.group("nick").strip()
        lock_type = match.group("lock_type").strip()
        success = match.group("success") == "Yes"
        elapsed = int(match.group("minutes")) * 60 + int(match.group("seconds"))
        entries.append((nick, lock_type, success, elapsed))
    return entries

# --- POBRANIE LISTY PLIKÓW Z FTP ---
def list_log_files(ftp):
    files = []
    def parse_line(line):
        parts = line.split()
        if len(parts) >= 9:
            filename = parts[-1]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                files.append(filename)
    ftp.dir(parse_line)
    return files

# --- SPRAWDZENIE, CZY PLIK BYŁ JUŻ PRZETWORZONY ---
def was_file_processed(filename):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM processed_logs WHERE filename = %s", (filename,))
            return cur.fetchone() is not None

# --- ZAPIS INFORMACJI, ŻE PLIK ZOSTAŁ PRZETWORZONY ---
def mark_file_as_processed(filename):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO processed_logs (filename) VALUES (%s) ON CONFLICT DO NOTHING", (filename,))
        conn.commit()

# --- POBRANIE I PARSOWANIE LOGÓW ---
def fetch_and_parse_logs():
    all_entries = []
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOGS_PATH)
        files = list_log_files(ftp)
        print(f"[DEBUG] Znaleziono plików: {len(files)}")

        for filename in files:
            if was_file_processed(filename):
                print(f"[INFO] Pomijam już przetworzony plik: {filename}")
                continue
            print(f"[INFO] Przetwarzam plik: {filename}")
            with io.BytesIO() as bio:
                ftp.retrbinary(f"RETR {filename}", bio.write)
                content = bio.getvalue().decode("utf-16-le", errors="ignore")
                entries = parse_log_content(content)
                print(f"[DEBUG] {filename} -> {len(entries)} wpisów")
                all_entries.extend(entries)
                mark_file_as_processed(filename)
    return all_entries

# --- ZAPIS DO BAZY ---
def save_entries(entries):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO lockpick_stats (nick, lock_type, success, elapsed_seconds)
                VALUES (%s, %s, %s, %s)
            """, entries)
        conn.commit()

# --- GENEROWANIE I WYSYŁKA TABELI ---
def generate_and_send_summary():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, lock_type,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE success) AS successes,
                    COUNT(*) FILTER (WHERE NOT success) AS failures,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE success) / COUNT(*), 1) AS effectiveness,
                    ROUND(AVG(elapsed_seconds)::numeric, 1) AS avg_time
                FROM lockpick_stats
                GROUP BY nick, lock_type
                ORDER BY effectiveness DESC, total DESC
            """)
            rows = cur.fetchall()

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność (%)", "Średni czas (s)"]
    col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]
    header_row = " | ".join(h.center(col_widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * w for w in col_widths)
    data_rows = [
        " | ".join(str(col).center(col_widths[i]) for i, col in enumerate(row))
        for row in rows
    ]
    table = "```\n" + header_row + "\n" + separator + "\n" + "\n".join(data_rows) + "\n```"

    if rows:
        requests.post(WEBHOOK_URL, json={"content": table})
    else:
        print("[INFO] Brak danych do wysłania.")

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    print("[DEBUG] Start programu")
    init_db()
    entries = fetch_and_parse_logs()
    if entries:
        print(f"[DEBUG] Zapisuję {len(entries)} wpisów do bazy danych")
        save_entries(entries)
        generate_and_send_summary()
    else:
        print("[INFO] Brak nowych wpisów do przetworzenia.")
