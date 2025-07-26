import ftplib
import io
import os
import re
import psycopg2
import time
import requests
from tabulate import tabulate
from datetime import datetime

# --- KONFIGURACJA ---
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

LOG_FILENAME_PATTERN = re.compile(r"gameplay_.*\.log$", re.IGNORECASE)

LOCKPICK_PATTERN = re.compile(
    r"User: (?P<nick>.*?) \(\d+, \d+\).*?Success: (?P<result>Yes|No).*?Elapsed time: (?P<time>[\d.]+).*?Target object: (?P<castle>.*?)\(",
    re.DOTALL
)

# --- FUNKCJE ---

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    return ftp

def get_ftp_log_files():
    ftp = connect_ftp()
    files = []

    def parse_line(line):
        parts = line.split()
        filename = parts[-1]
        if LOG_FILENAME_PATTERN.match(filename):
            files.append(filename)

    ftp.retrlines("LIST", parse_line)
    ftp.quit()
    return files

def download_log_file(filename):
    ftp = connect_ftp()
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    ftp.quit()
    bio.seek(0)
    return bio.read().decode("utf-16-le", errors="ignore")

def parse_lockpick_entries(log_data):
    return [
        match.groupdict()
        for match in LOCKPICK_PATTERN.finditer(log_data)
    ]

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            castle TEXT,
            result TEXT,
            time FLOAT
        );
    """)
    try:
        cur.execute("ALTER TABLE lockpick_stats ADD COLUMN result TEXT;")
        print("[INFO] Dodano brakującą kolumnę 'result'...")
    except psycopg2.errors.DuplicateColumn:
        pass
    conn.commit()
    return conn, cur

def entry_exists(cur, entry):
    cur.execute("""
        SELECT 1 FROM lockpick_stats
        WHERE nick = %s AND castle = %s AND result = %s AND time = %s
        LIMIT 1
    """, (entry["nick"], entry["castle"], entry["result"], float(entry["time"])))
    return cur.fetchone() is not None

def save_entries(cur, conn, entries):
    new_count = 0
    for entry in entries:
        if not entry_exists(cur, entry):
            cur.execute("""
                INSERT INTO lockpick_stats (nick, castle, result, time)
                VALUES (%s, %s, %s, %s)
            """, (entry["nick"], entry["castle"], entry["result"], float(entry["time"])))
            new_count += 1
    conn.commit()
    return new_count

def get_statistics(cur):
    cur.execute("SELECT nick, castle, result, time FROM lockpick_stats")
    rows = cur.fetchall()

    stats = {}
    for nick, castle, result, time_val in rows:
        key = (nick, castle)
        if key not in stats:
            stats[key] = {"total": 0, "success": 0, "fail": 0, "times": []}
        stats[key]["total"] += 1
        if result == "Yes":
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1
        stats[key]["times"].append(time_val)

    result_rows = []
    for (nick, castle), data in stats.items():
        success_rate = data["success"] / data["total"] * 100
        avg_time = sum(data["times"]) / len(data["times"])
        result_rows.append([
            nick,
            castle,
            data["total"],
            data["success"],
            data["fail"],
            f"{success_rate:.2f}%",
            f"{avg_time:.2f}s"
        ])

    result_rows.sort(key=lambda row: (-row[3], row[0]))
    return result_rows

def send_to_webhook(stats):
    if not stats:
        return

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = tabulate(stats, headers=headers, tablefmt="github", stralign="center", numalign="center")
    data = {"content": f"```\n{table}\n```"}
    requests.post(WEBHOOK_URL, json=data)

# --- GŁÓWNA PĘTLA ---

def main():
    print("[INFO] Inicjalizacja bazy...")
    conn, cur = init_db()

    print("[INFO] Pobieranie logów...")
    files = get_ftp_log_files()
    print(f"[DEBUG] Liczba logów: {len(files)}")

    all_entries = []
    for file in files:
        log_data = download_log_file(file)
        entries = parse_lockpick_entries(log_data)
        all_entries.extend(entries)

    print("[INFO] Parsowanie danych...")
    print(f"[DEBUG] Sparsowane wpisy lockpick: {len(all_entries)}")

    if not all_entries:
        print("[INFO] Brak danych lockpick.")
        return

    print("[INFO] Zapisywanie do bazy...")
    new_count = save_entries(cur, conn, all_entries)
    print(f"[INFO] Nowe wpisy zapisane: {new_count}")

    if new_count > 0:
        print("[INFO] Generowanie statystyk...")
        stats = get_statistics(cur)
        print("[INFO] Wysyłanie danych na webhook...")
        send_to_webhook(stats)
    else:
        print("[INFO] Brak nowych wpisów - webhook pominięty.")

    conn.close()

if __name__ == "__main__":
    main()
