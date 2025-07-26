# main.py

import os
import re
import time
import ftplib
import psycopg2
import threading
from flask import Flask
from datetime import datetime
from collections import defaultdict
import requests

# --- KONFIGURACJA ---

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

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def get_conn():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        sslmode=DB_CONFIG["sslmode"]
    )

def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_stats (
                    nick TEXT,
                    zamek TEXT,
                    success BOOLEAN,
                    time FLOAT,
                    log_file TEXT,
                    UNIQUE(nick, zamek, success, time, log_file)
                )
            """)
            conn.commit()
    print("[INFO] Baza danych gotowa.")

def list_log_files():
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)
        files = ftp.nlst()
        return sorted([f for f in files if f.startswith("gameplay_") and f.endswith(".log")])

def download_log_file(filename):
    print(f"[INFO] Pobieranie logu: {filename}")
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)
        data = []
        ftp.retrbinary(f"RETR {filename}", data.append)
        content = b"".join(data).decode("utf-16-le", errors="ignore")
        return content

def parse_log(content):
    entries = []
    for line in content.splitlines():
        if "Lockpicking" in line and "by" in line and "seconds" in line:
            match = re.search(
                r'Lockpicking (succeeded|failed) on (\w+) by (.+?) in ([\d.]+) seconds',
                line
            )
            if match:
                success = match.group(1) == "succeeded"
                zamek = match.group(2)
                nick = match.group(3)
                try:
                    czas = float(match.group(4).replace(",", ".").strip("."))
                    entries.append((nick, zamek, success, czas))
                except ValueError:
                    continue
    return entries

def save_entries_to_db(entries, log_file):
    new = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for nick, zamek, success, czas in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpick_stats (nick, zamek, success, time, log_file)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (nick, zamek, success, czas, log_file))
                    if cur.rowcount:
                        new += 1
                except Exception as e:
                    print(f"[ERROR] Błąd zapisu: {e}")
            conn.commit()
    print(f"[INFO] Zapisano {new} nowych wpisów z {log_file}")
    return new > 0

def generate_report():
    print("[INFO] Generowanie raportu...")
    rows = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, zamek,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE success) AS success_count,
                       COUNT(*) FILTER (WHERE NOT success) AS fail_count,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE success) / NULLIF(COUNT(*),0), 2) AS skutecznosc,
                       ROUND(AVG(time), 2) AS avg_time
                FROM lockpick_stats
                GROUP BY nick, zamek
                ORDER BY skutecznosc DESC, avg_time
            """)
            rows = cur.fetchall()

    if not rows:
        print("[INFO] Brak danych do raportu.")
        return None

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]

    def format_row(row):
        return "| " + " | ".join(str(cell).center(width) for cell, width in zip(row, col_widths)) + " |"

    table = format_row(headers) + "\n"
    table += "|-" + "-|-".join("-" * w for w in col_widths) + "-|\n"
    for row in rows:
        table += format_row(row) + "\n"

    print("[INFO] Raport gotowy.")
    return table

def send_webhook(message):
    try:
        requests.post(WEBHOOK_URL, json={"content": f"```\n{message}```"})
        print("[INFO] Wysłano dane na webhook.")
    except Exception as e:
        print(f"[ERROR] Błąd webhooka: {e}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    processed_logs = set()

    while True:
        try:
            all_logs = list_log_files()
            new_found = False
            for log_file in all_logs:
                if log_file in processed_logs:
                    continue
                content = download_log_file(log_file)
                entries = parse_log(content)
                print(f"[DEBUG] {log_file}: znaleziono {len(entries)} wpisów.")
                if not entries:
                    continue
                has_new = save_entries_to_db(entries, log_file)
                if has_new:
                    new_found = True
                processed_logs.add(log_file)

            if new_found:
                report = generate_report()
                if report:
                    send_webhook(report)
            else:
                print("[INFO] Brak nowych danych do przetworzenia.")

        except Exception as e:
            print(f"[ERROR] Błąd główny: {e}")

        time.sleep(60)

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
