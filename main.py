import os
import re
import time
import ftplib
import psycopg2
import statistics
import requests
from datetime import datetime
from io import BytesIO
from flask import Flask
from collections import defaultdict

# === [ KONFIGURACJA ] ===

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

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# === [ FUNKCJE POMOCNICZE ] ===

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def initialize_database():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpick_stats (
                nick TEXT,
                castle TEXT,
                success BOOLEAN,
                time FLOAT,
                log_line TEXT UNIQUE
            );
        """)
        conn.commit()
        print("[INFO] Inicjalizacja bazy danych...")

def download_log_files():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    print("[INFO] Połączono z FTP. Listowanie plików...")

    # Listuj pliki ręcznie, bo serwer nie wspiera NLST
    filenames = []
    ftp.retrlines('LIST', lambda line: filenames.append(line))

    gameplay_logs = []
    for entry in filenames:
        parts = entry.split()
        filename = parts[-1]
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            gameplay_logs.append(filename)

    print(f"[INFO] Znaleziono {len(gameplay_logs)} plików gameplay_*.log")

    log_contents = {}
    for filename in gameplay_logs:
        bio = BytesIO()
        try:
            ftp.retrbinary(f"RETR {filename}", bio.write)
            bio.seek(0)
            content = bio.read().decode("utf-16-le", errors="ignore")
            log_contents[filename] = content
            print(f"[DEBUG] Wczytano {filename}, {len(content)} znaków")
        except Exception as e:
            print(f"[ERROR] Nie można pobrać {filename}: {e}")
    ftp.quit()
    return log_contents

def parse_logs(log_contents):
    pattern = re.compile(
        r'(?P<timestamp>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}).*?\[Lockpicking\] (?P<nick>.*?) próbował otworzyć zamek (?P<castle>.*?) - (?P<result>Sukces|Porażka) \(czas: (?P<time>\d+(?:\.\d+)?)s\)'
    )
    entries = []

    for filename, content in log_contents.items():
        for line in content.splitlines():
            match = pattern.search(line)
            if match:
                data = match.groupdict()
                entries.append({
                    "timestamp": data["timestamp"],
                    "nick": data["nick"],
                    "castle": data["castle"],
                    "success": data["result"] == "Sukces",
                    "time": float(data["time"]),
                    "log_line": line.strip()
                })
    print(f"[INFO] Wyodrębniono {len(entries)} wpisów lockpick z logów.")
    return entries

def insert_new_entries(entries):
    new_count = 0
    with connect_db() as conn, conn.cursor() as cur:
        for entry in entries:
            try:
                cur.execute("""
                    INSERT INTO lockpick_stats (nick, castle, success, time, log_line)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (log_line) DO NOTHING
                """, (entry["nick"], entry["castle"], entry["success"], entry["time"], entry["log_line"]))
                if cur.rowcount > 0:
                    new_count += 1
            except Exception as e:
                print(f"[ERROR] Błąd przy dodawaniu wpisu: {e}")
        conn.commit()
    print(f"[INFO] Dodano {new_count} nowych wpisów do bazy.")

def generate_statistics():
    stats = defaultdict(lambda: {"total": 0, "success": 0, "fail": 0, "times": []})
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT nick, castle, success, time FROM lockpick_stats")
        for nick, castle, success, t in cur.fetchall():
            key = (nick, castle)
            stats[key]["total"] += 1
            if success:
                stats[key]["success"] += 1
            else:
                stats[key]["fail"] += 1
            stats[key]["times"].append(t)

    lines = []
    header = f"| {'Nick':^16} | {'Zamek':^16} | {'Ilość prób':^12} | {'Udane':^6} | {'Nieudane':^9} | {'Skuteczność':^12} | {'Śr. czas':^8} |"
    lines.append(header)
    lines.append("|" + "-" * (len(header) - 2) + "|")

    for (nick, castle), data in stats.items():
        skutecznosc = f"{(data['success'] / data['total'] * 100):.1f}%" if data["total"] else "0.0%"
        sredni = f"{statistics.mean(data['times']):.2f}" if data["times"] else "-"
        lines.append(f"| {nick:^16} | {castle:^16} | {data['total']:^12} | {data['success']:^6} | {data['fail']:^9} | {skutecznosc:^12} | {sredni:^8} |")

    return "```\n" + "\n".join(lines) + "\n```"

def send_to_discord(report):
    try:
        requests.post(WEBHOOK_URL, json={"content": report})
        print("[INFO] Wysłano statystyki na Discord webhook.")
    except Exception as e:
        print(f"[ERROR] Nie udało się wysłać raportu: {e}")

# === [ GŁÓWNA PĘTLA ] ===

def main_loop():
    print("[DEBUG] Start main_loop")
    initialize_database()
    log_contents = download_log_files()
    parsed_entries = parse_logs(log_contents)
    insert_new_entries(parsed_entries)

    if parsed_entries:
        report = generate_statistics()
        send_to_discord(report)
    else:
        print("[INFO] Brak nowych wpisów do przetworzenia.")

# === [ START APP ] ===

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
