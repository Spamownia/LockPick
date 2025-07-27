import os
import re
import time
import ftplib
import hashlib
import psycopg2
import requests
from io import BytesIO
from flask import Flask

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

LOCKPICK_REGEX = re.compile(
    r"LOCKPICK:\s+(?P<nick>.+?)\s+tried to pick the lock of (?P<zamek>.+?)\.\s+Result:\s+(?P<wynik>SUCCESS|FAILURE)(?:,\s+Time:\s+(?P<czas>[\d.]+)s)?"
)

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def init_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gameplay_logs (
                    hash TEXT PRIMARY KEY,
                    nick TEXT,
                    zamek TEXT,
                    wynik TEXT,
                    czas REAL
                );
            """)
        conn.commit()
    print("[INFO] Inicjalizacja bazy danych...")

def parse_log(content):
    entries = []
    for match in LOCKPICK_REGEX.finditer(content):
        nick = match.group("nick")
        zamek = match.group("zamek")
        wynik = match.group("wynik")
        czas = match.group("czas")
        czas_float = float(czas) if czas else None
        hash_input = f"{nick}{zamek}{wynik}{czas}"
        hash_val = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
        entries.append((hash_val, nick, zamek, wynik, czas_float))
    return entries

def get_log_files():
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOG_PATH)
            files = []
            ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
            log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
            print(f"[INFO] Znaleziono {len(log_files)} plików gameplay_*.log")
            return log_files
    except Exception as e:
        print(f"[ERROR] FTP get_log_files: {e}")
        return []

def download_file(filename):
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOG_PATH)
            buffer = BytesIO()
            ftp.retrbinary(f"RETR {filename}", buffer.write)
            content = buffer.getvalue().decode("utf-16le")
            return content
    except Exception as e:
        print(f"[ERROR] Błąd pobierania {filename}: {e}")
        return ""

def insert_entries(entries):
    new = 0
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for entry in entries:
                try:
                    cur.execute("""
                        INSERT INTO gameplay_logs (hash, nick, zamek, wynik, czas)
                        VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
                    """, entry)
                    if cur.rowcount > 0:
                        new += 1
                except Exception as e:
                    print(f"[WARN] Błąd INSERT: {e}")
            conn.commit()
    print(f"[INFO] Nowe wpisy dodane: {new}")
    return new > 0

def generate_summary():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, zamek,
                    COUNT(*) AS proby,
                    COUNT(*) FILTER (WHERE wynik = 'SUCCESS') AS udane,
                    COUNT(*) FILTER (WHERE wynik = 'FAILURE') AS nieudane,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE wynik = 'SUCCESS') / COUNT(*), 1) AS skutecznosc,
                    ROUND(AVG(czas), 2) AS sredni_czas
                FROM gameplay_logs
                GROUP BY nick, zamek
                ORDER BY skutecznosc DESC NULLS LAST, sredni_czas ASC NULLS LAST;
            """)
            rows = cur.fetchall()

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = [" | ".join(headers)]
    table.append("-|-".join("-" * len(h) for h in headers))
    for row in rows:
        table.append(" | ".join(str(cell) if cell is not None else "-" for cell in row))
    return "\n".join(table)

def send_webhook(content):
    try:
        requests.post(WEBHOOK_URL, json={"content": f"```\n{content}\n```"})
        print("[INFO] Webhook wysłany.")
    except Exception as e:
        print(f"[ERROR] Webhook error: {e}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    processed_files = set()

    while True:
        try:
            log_files = get_log_files()
            if not log_files:
                print("[WARN] Brak plików logów.")
                time.sleep(60)
                continue

            new_entries = []
            for filename in log_files:
                if filename not in processed_files:
                    print(f"[DEBUG] Przetwarzanie {filename}")
                    content = download_file(filename)
                    parsed = parse_log(content)
                    new_entries.extend(parsed)
                    processed_files.add(filename)

            if new_entries:
                print(f"[DEBUG] Łącznie nowych wpisów: {len(new_entries)}")
                if insert_entries(new_entries):
                    summary = generate_summary()
                    send_webhook(summary)
                else:
                    print("[INFO] Brak nowych wpisów do bazy.")
            else:
                print("[INFO] Brak nowych danych w logach.")

        except Exception as e:
            print(f"[ERROR] main_loop: {e}")
        time.sleep(60)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
