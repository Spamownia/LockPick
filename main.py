import os
import io
import re
import time
import ftplib
import psycopg2
import threading
import statistics
import requests
from flask import Flask
from collections import defaultdict

# ===== KONFIGURACJA =====
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

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gameplay_logs (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            zamek TEXT,
            sukces BOOLEAN,
            czas REAL,
            unikalny_id TEXT UNIQUE
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DEBUG] Inicjalizacja bazy danych...")

def parse_logs(content):
    data = []
    lines = content.splitlines()
    for line in lines:
        match = re.search(r'(.+?) - Player (.+?) tried to pick lock on (.+?) - Result: (Success|Fail) - Time: ([\d.]+)s', line)
        if match:
            timestamp, nick, zamek, result, czas = match.groups()
            sukces = (result == "Success")
            uid = f"{timestamp}_{nick}_{zamek}_{result}_{czas}"
            data.append((nick, zamek, sukces, float(czas), uid))
    return data

def fetch_existing_ids():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT unikalny_id FROM gameplay_logs;")
    existing_ids = set(row[0] for row in cur.fetchall())
    cur.close()
    conn.close()
    return existing_ids

def save_to_db(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_count = 0
    for entry in entries:
        try:
            cur.execute("""
                INSERT INTO gameplay_logs (nick, zamek, sukces, czas, unikalny_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (unikalny_id) DO NOTHING;
            """, entry)
            if cur.rowcount > 0:
                new_count += 1
        except Exception as e:
            print(f"[ERROR] Błąd zapisu do bazy: {e}")
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Zapisano {new_count} nowych wpisów do bazy.")

def fetch_stats():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, zamek,
            COUNT(*) as proby,
            SUM(CASE WHEN sukces THEN 1 ELSE 0 END) as udane,
            SUM(CASE WHEN NOT sukces THEN 1 ELSE 0 END) as nieudane,
            ROUND(100.0 * SUM(CASE WHEN sukces THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2) as skutecznosc,
            ROUND(AVG(czas), 2) as sredni_czas
        FROM gameplay_logs
        GROUP BY nick, zamek
        ORDER BY skutecznosc DESC NULLS LAST, sredni_czas ASC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def format_table(rows):
    headers = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = [headers]
    for row in rows:
        table.append([str(cell) for cell in row])
    col_widths = [max(len(row[i]) for row in table) for i in range(len(headers))]
    formatted = "\n".join(" | ".join(row[i].center(col_widths[i]) for i in range(len(headers))) for row in table)
    return f"```\n{formatted}\n```"

def send_to_webhook(text):
    payload = {"content": text}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Webhook status: {response.status_code}")

def download_all_logs():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)
        files = []
        ftp.retrlines('LIST', lambda x: files.append(x.split()[-1]))
        log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
        print(f"[DEBUG] Znaleziono {len(log_files)} plików logów gameplay_*.log")

        all_entries = []
        for filename in log_files:
            print(f"[DEBUG] Przetwarzanie pliku: {filename}")
            with io.BytesIO() as bio:
                ftp.retrbinary(f"RETR {filename}", bio.write)
                content = bio.getvalue().decode("utf-16-le", errors="ignore")
                entries = parse_logs(content)
                print(f"[DEBUG] Znaleziono {len(entries)} wpisów w pliku {filename}")
                all_entries.extend(entries)

        ftp.quit()
        return all_entries

    except Exception as e:
        print(f"[ERROR] Błąd pobierania z FTP: {e}")
        return []

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()

    while True:
        entries = download_all_logs()
        if not entries:
            print("[DEBUG] Brak logów do przetworzenia.")
            time.sleep(60)
            continue

        existing_ids = fetch_existing_ids()
        new_entries = [entry for entry in entries if entry[4] not in existing_ids]

        if new_entries:
            save_to_db(new_entries)
            stats = fetch_stats()
            table_text = format_table(stats)
            send_to_webhook(table_text)
        else:
            print("[DEBUG] Brak nowych danych do przetworzenia.")

        time.sleep(60)

def start():
    threading.Thread(target=main_loop).start()

if __name__ == "__main__":
    start()
    app.run(host='0.0.0.0', port=3000)
