import os
import io
import re
import time
import ftplib
import psycopg2
import requests
from datetime import datetime
from flask import Flask

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

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            zamek TEXT,
            powodzenie BOOLEAN,
            czas REAL,
            raw TEXT UNIQUE
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[INFO] Inicjalizacja bazy danych...")

def parse_log_line(line):
    match = re.search(r"\[(.*?)\] Player \[(.*?)\] tried to pick \[(.*?)\] - (Succeeded|Failed) in ([\d.]+)s", line)
    if match:
        nick = match.group(2)
        zamek = match.group(3)
        success = match.group(4) == "Succeeded"
        czas = float(match.group(5))
        return nick, zamek, success, czas, line.strip()
    return None

def process_log_file(file_bytes):
    lines = file_bytes.decode("utf-16-le").splitlines()
    entries = [parse_log_line(line) for line in lines]
    return [entry for entry in entries if entry is not None]

def fetch_log_files():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    
    log_files = []
    try:
        files = []
        ftp.retrlines('LIST', files.append)
        for line in files:
            parts = line.split()
            filename = parts[-1]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                log_files.append(filename)
    except Exception as e:
        print(f"[ERROR] Nie udało się pobrać listy plików: {e}")
        ftp.quit()
        return []

    print(f"[INFO] Znaleziono {len(log_files)} plików logów.")
    contents = []
    for fname in log_files:
        try:
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {fname}", bio.write)
            contents.append(bio.getvalue())
            print(f"[DEBUG] Pobrano plik: {fname}")
        except Exception as e:
            print(f"[ERROR] Nie udało się pobrać {fname}: {e}")

    ftp.quit()
    return contents

def save_to_db(entries):
    conn = connect_db()
    cur = conn.cursor()
    inserted = 0
    for nick, zamek, success, czas, raw in entries:
        try:
            cur.execute("""
                INSERT INTO lockpick_logs (nick, zamek, powodzenie, czas, raw)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (raw) DO NOTHING
            """, (nick, zamek, success, czas, raw))
            inserted += cur.rowcount
        except Exception as e:
            print(f"[ERROR] Błąd zapisu do bazy: {e}")
    conn.commit()
    cur.close()
    conn.close()
    print(f"[INFO] Dodano {inserted} nowych wpisów do bazy.")

def fetch_stats():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, zamek,
               COUNT(*) AS proby,
               COUNT(*) FILTER (WHERE powodzenie) AS udane,
               COUNT(*) FILTER (WHERE NOT powodzenie) AS nieudane,
               ROUND(100.0 * COUNT(*) FILTER (WHERE powodzenie) / NULLIF(COUNT(*),0), 1) AS skutecznosc,
               ROUND(AVG(czas), 2) AS sredni_czas
        FROM lockpick_logs
        GROUP BY nick, zamek
        ORDER BY skutecznosc DESC NULLS LAST, sredni_czas ASC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def format_table(rows):
    headers = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Śr. czas"]
    table = [headers]
    for row in rows:
        table.append([str(r) for r in row])

    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*table)]
    lines = []
    for row in table:
        line = " | ".join(cell.center(width) for cell, width in zip(row, col_widths))
        lines.append(line)

    return "```\n" + "\n".join(lines) + "\n```"

def send_webhook(table_text):
    payload = {"content": table_text}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code != 204:
        print(f"[ERROR] Nie udało się wysłać webhooka: {response.text}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    log_contents = fetch_log_files()
    total_entries = []
    for content in log_contents:
        entries = process_log_file(content)
        total_entries.extend(entries)

    print(f"[INFO] Przetworzono {len(total_entries)} wpisów z logów.")
    save_to_db(total_entries)
    stats = fetch_stats()
    if stats:
        tabela = format_table(stats)
        send_webhook(tabela)
    else:
        print("[INFO] Brak danych do wysłania.")

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
