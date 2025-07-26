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

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# Dane konfiguracyjne
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            nick TEXT,
            castle TEXT,
            result TEXT,
            duration FLOAT,
            log_hash TEXT PRIMARY KEY
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def fetch_log_files():
    print("[DEBUG] Start fetch_log_files()")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)

    files = []
    try:
        files = [entry[0] for entry in ftp.mlsd() if entry[0].startswith("gameplay_") and entry[0].endswith(".log")]
    except ftplib.error_perm as e:
        print(f"[ERROR] FTP MLSD failed: {e}")

    new_entries = []
    for filename in files:
        print(f"[DEBUG] Przetwarzanie pliku: {filename}")
        bio = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {filename}", bio.write)
        except Exception as e:
            print(f"[ERROR] Nie udało się pobrać pliku {filename}: {e}")
            continue
        content = bio.getvalue().decode('utf-16-le', errors='ignore')
        entries = parse_log(content)
        new_entries.extend(entries)
    ftp.quit()
    return new_entries

def parse_log(content):
    pattern = r'LOCKPICK: Player (.*?) tried to lockpick (.*?) and (SUCCEEDED|FAILED) in ([\d.]+) seconds'
    matches = re.findall(pattern, content)
    parsed = []
    for match in matches:
        nick, castle, result, duration = match
        raw_line = f"{nick}-{castle}-{result}-{duration}"
        parsed.append({
            "nick": nick.strip(),
            "castle": castle.strip(),
            "result": result.strip(),
            "duration": float(duration),
            "log_hash": str(hash(raw_line))
        })
    print(f"[DEBUG] Wyodrębniono {len(parsed)} wpisów z logu")
    return parsed

def save_to_db(entries):
    print(f"[DEBUG] Zapisuję {len(entries)} wpisów do bazy...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    inserted = 0
    for entry in entries:
        try:
            cur.execute("""
                INSERT INTO lockpick_logs (nick, castle, result, duration, log_hash)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (log_hash) DO NOTHING
            """, (entry["nick"], entry["castle"], entry["result"], entry["duration"], entry["log_hash"]))
            inserted += cur.rowcount
        except Exception as e:
            print(f"[ERROR] Błąd przy dodawaniu wpisu: {e}")
    conn.commit()
    cur.close()
    conn.close()
    print(f"[INFO] Dodano {inserted} nowych wpisów do bazy.")
    return inserted > 0

def generate_stats():
    print("[DEBUG] Generuję statystyki...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, castle,
               COUNT(*) as total,
               COUNT(*) FILTER (WHERE result = 'SUCCEEDED') as success,
               COUNT(*) FILTER (WHERE result = 'FAILED') as fail,
               ROUND(AVG(duration)::numeric, 2) as avg_time
        FROM lockpick_logs
        GROUP BY nick, castle
        ORDER BY success DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = [headers]
    for row in rows:
        nick, castle, total, success, fail, avg_time = row
        eff = f"{round(success / total * 100)}%" if total > 0 else "0%"
        table.append([nick, castle, str(total), str(success), str(fail), eff, str(avg_time)])

    max_lens = [max(len(row[i]) for row in table) for i in range(len(headers))]
    formatted = ""
    for row in table:
        formatted += " | ".join(f"{col:^{max_lens[i]}}" for i, col in enumerate(row)) + "\n"
    print("[DEBUG] Statystyki wygenerowane.")
    return f"```\n{formatted}```"

def send_webhook(message):
    print("[DEBUG] Wysyłam wiadomość do webhooka...")
    response = requests.post(WEBHOOK_URL, json={"content": message})
    print(f"[INFO] Webhook status: {response.status_code}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        print("[DEBUG] Sprawdzam nowe logi...")
        new_entries = fetch_log_files()
        if new_entries:
            print(f"[INFO] Znaleziono {len(new_entries)} wpisów")
            if save_to_db(new_entries):
                stats = generate_stats()
                send_webhook(stats)
        else:
            print("[INFO] Brak nowych wpisów w logach.")
        time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=main_loop).start()
    app.run(host='0.0.0.0', port=3000)
