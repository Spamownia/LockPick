import ftplib
import os
import re
import psycopg2
import requests
import threading
import time
import io
import codecs
from datetime import datetime
from flask import Flask

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

# === FLASK (do pingowania) ===

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

threading.Thread(target=lambda: app.run(host="0.0.0.0", port=3000)).start()

# === FUNKCJE ===

def connect_db():
    print("[INFO] Inicjalizacja bazy danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            zamek TEXT,
            sukces BOOLEAN,
            czas REAL,
            data TIMESTAMP,
            PRIMARY KEY (nick, zamek, data)
        )
    """)
    conn.commit()
    return conn

def parse_line(line):
    match = re.search(r'\[(.*?)\] Character (.*?) tried to pick lock on (.*?) and (succeeded|failed) in ([\d\.]+) seconds', line)
    if match:
        timestamp = datetime.strptime(match.group(1), "%Y.%m.%d-%H.%M.%S")
        nick = match.group(2)
        zamek = match.group(3)
        sukces = match.group(4) == "succeeded"
        czas = float(match.group(5))
        return (nick, zamek, sukces, czas, timestamp)
    return None

def fetch_log_files():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        print("[INFO] Połączono z FTP.")

        try:
            ftp.cwd(FTP_LOG_DIR)
        except Exception as e:
            print(f"[ERROR] Nie można przejść do katalogu logów: {e}")
            ftp.quit()
            return []

        try:
            files = []
            ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
        except Exception as e:
            print(f"[ERROR] LIST nie działa, użycie work-around: {e}")
            files = [name for name in ftp.nlst() if name.startswith("gameplay_") and name.endswith(".log")]

        gameplay_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
        print(f"[INFO] Znaleziono plików: {len(gameplay_files)}")

        new_entries = []
        for filename in gameplay_files:
            with io.BytesIO() as bio:
                try:
                    ftp.retrbinary(f"RETR {filename}", bio.write)
                    content = bio.getvalue().decode("utf-16le", errors="ignore")
                    for line in content.splitlines():
                        parsed = parse_line(line)
                        if parsed:
                            new_entries.append(parsed)
                except Exception as e:
                    print(f"[ERROR] Błąd przy pobieraniu/parsingu {filename}: {e}")
        ftp.quit()
        print(f"[INFO] Przetworzono wpisów: {len(new_entries)}")
        return new_entries
    except Exception as e:
        print(f"[ERROR] Błąd FTP: {e}")
        return []

def update_database(conn, entries):
    if not entries:
        print("[INFO] Brak nowych wpisów do aktualizacji.")
        return False
    cur = conn.cursor()
    new_count = 0
    for entry in entries:
        try:
            cur.execute("""
                INSERT INTO lockpicking (nick, zamek, sukces, czas, data)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, entry)
            if cur.rowcount > 0:
                new_count += 1
        except Exception as e:
            print(f"[ERROR] Błąd zapisu do bazy: {e}")
    conn.commit()
    print(f"[INFO] Dodano {new_count} nowych rekordów.")
    return new_count > 0

def generate_table(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, zamek,
               COUNT(*) as total,
               COUNT(*) FILTER (WHERE sukces) as success,
               COUNT(*) FILTER (WHERE NOT sukces) as failure,
               ROUND(100.0 * COUNT(*) FILTER (WHERE sukces) / COUNT(*), 2) as skutecznosc,
               ROUND(AVG(czas), 2) as sredni_czas
        FROM lockpicking
        GROUP BY nick, zamek
        ORDER BY skutecznosc DESC, total DESC
    """)
    rows = cur.fetchall()

    headers = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]

    def format_row(row):
        return "| " + " | ".join(f"{str(cell).center(col_widths[i])}" for i, cell in enumerate(row)) + " |"

    lines = ["```"]
    lines.append(format_row(headers))
    lines.append("|" + "|".join("-" * (w + 2) for w in col_widths) + "|")
    for row in rows:
        lines.append(format_row(row))
    lines.append("```")
    return "\n".join(lines)

def send_to_webhook(table):
    print("[INFO] Wysyłam dane na webhook...")
    try:
        requests.post(WEBHOOK_URL, json={"content": table})
        print("[INFO] Wysłano dane.")
    except Exception as e:
        print(f"[ERROR] Nie udało się wysłać webhooka: {e}")

# === GŁÓWNA PĘTLA ===

def main_loop():
    print("[DEBUG] Start main_loop")
    conn = connect_db()
    while True:
        entries = fetch_log_files()
        if update_database(conn, entries):
            table = generate_table(conn)
            send_to_webhook(table)
        else:
            print("[INFO] Brak nowych danych.")
        time.sleep(60)

threading.Thread(target=main_loop).start()
