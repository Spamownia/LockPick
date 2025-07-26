import os
import re
import ftplib
import psycopg2
import requests
import threading
import time
import io
import codecs
from datetime import datetime
from flask import Flask

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

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

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_stats (
            nick TEXT,
            zamek TEXT,
            sukces BOOLEAN,
            czas REAL,
            PRIMARY KEY (nick, zamek, sukces, czas)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def parse_log(content):
    lines = content.splitlines()
    data = []
    for line in lines:
        match = re.search(r"\[(.*?)\] \[Lockpicking\] (.*?) tried to pick (.*?) and (SUCCEEDED|FAILED) in ([\d\.]+)s", line)
        if match:
            _, nick, zamek, result, time_taken = match.groups()
            data.append((nick, zamek, result == "SUCCEEDED", float(time_taken)))
    return data

def insert_new_entries(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_entries = 0
    for entry in entries:
        try:
            cur.execute("""
                INSERT INTO lockpicking_stats (nick, zamek, sukces, czas)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, entry)
            if cur.rowcount:
                new_entries += 1
        except Exception as e:
            print("[ERROR] Błąd przy zapisie do DB:", e)
    conn.commit()
    cur.close()
    conn.close()
    return new_entries

def summarize_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, zamek,
            COUNT(*) AS ilosc,
            COUNT(*) FILTER (WHERE sukces) AS udane,
            COUNT(*) FILTER (WHERE NOT sukces) AS nieudane,
            ROUND(AVG(czas)::numeric, 2) AS sredni_czas,
            ROUND(100.0 * COUNT(*) FILTER (WHERE sukces)::numeric / NULLIF(COUNT(*), 0), 1) AS skutecznosc
        FROM lockpicking_stats
        GROUP BY nick, zamek
        ORDER BY skutecznosc DESC NULLS LAST
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def send_to_discord(rows):
    if not rows:
        return
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]

    def format_row(row):
        return " | ".join(str(col).center(width) for col, width in zip(row, col_widths))

    table = "```\n" + format_row(headers) + "\n" + "-" * sum(col_widths) + "\n"
    for row in rows:
        table += format_row(row) + "\n"
    table += "```"

    response = requests.post(WEBHOOK_URL, json={"content": table})
    if response.status_code != 204:
        print(f"[ERROR] Nie udało się wysłać do Discorda: {response.status_code} - {response.text}")

def list_log_files(ftp):
    files = []

    def collect_filename(line):
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            name = parts[8]
            if name.startswith("gameplay_") and name.endswith(".log"):
                files.append(name)

    ftp.dir(FTP_LOG_PATH, collect_filename)
    return files

def fetch_log_files():
    print("[DEBUG] Pobieranie plików z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)

    files = list_log_files(ftp)
    print(f"[INFO] Znaleziono {len(files)} plików gameplay_*.log")

    all_entries = []
    for filename in files:
        print(f"[DEBUG] Przetwarzanie pliku: {filename}")
        try:
            buffer = io.BytesIO()
            ftp.retrbinary(f"RETR {FTP_LOG_PATH}{filename}", buffer.write)
            content = buffer.getvalue().decode("utf-16le", errors="ignore")
            entries = parse_log(content)
            all_entries.extend(entries)
            print(f"[DEBUG] → Znaleziono {len(entries)} wpisów w {filename}")
        except Exception as e:
            print(f"[ERROR] Błąd przy przetwarzaniu {filename}: {e}")
    ftp.quit()
    return all_entries

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()

    all_entries = fetch_log_files()
    print(f"[INFO] Suma wpisów do analizy: {len(all_entries)}")

    new_count = insert_new_entries(all_entries)
    if new_count > 0:
        print(f"[INFO] Dodano {new_count} nowych wpisów")
        rows = summarize_data()
        send_to_discord(rows)
    else:
        print("[INFO] Brak nowych danych do dodania")

threading.Thread(target=main_loop).start()
app.run(host="0.0.0.0", port=3000)
