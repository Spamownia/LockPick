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

processed_files = set()

def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            castle TEXT,
            success BOOLEAN,
            time FLOAT,
            timestamp TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def parse_log(content):
    pattern = re.compile(
        r"Lockpicking: \[(?P<nick>[^\]]+)] tried to open lock on (?P<castle>\w+)\. Result: (?P<result>\w+)\. Time: (?P<time>\d+\.\d+)s"
    )
    return pattern.findall(content)

def save_to_db(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for nick, castle, result, time_taken in entries:
        cur.execute("""
            INSERT INTO lockpicking (nick, castle, success, time)
            VALUES (%s, %s, %s, %s)
        """, (nick, castle, result.lower() == "success", float(time_taken)))
    conn.commit()
    cur.close()
    conn.close()

def generate_summary():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, castle,
               COUNT(*) as total,
               SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
               SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as fail_count,
               ROUND(100.0 * SUM(CASE WHEN success THEN 1 ELSE 0 END)/COUNT(*), 2) as accuracy,
               ROUND(AVG(time), 2) as avg_time
        FROM lockpicking
        GROUP BY nick, castle
        ORDER BY accuracy DESC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [max(len(str(x)) for x in col) for col in zip(*([headers] + rows))]

    table = "```"
    table += "\n" + " | ".join(header.center(width) for header, width in zip(headers, col_widths))
    table += "\n" + "-+-".join("-" * width for width in col_widths)
    for row in rows:
        table += "\n" + " | ".join(str(item).center(width) for item, width in zip(row, col_widths))
    table += "```"
    return table

def fetch_log_files():
    new_entries = []
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)

        files = []
        ftp.retrlines('LIST', lambda line: files.append(line.split()[-1]))
        log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

        for filename in log_files:
            if filename in processed_files:
                continue

            print(f"[INFO] Przetwarzanie pliku: {filename}")
            r = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", r.write)
            r.seek(0)
            content = r.read().decode('utf-16-le', errors='ignore')
            entries = parse_log(content)
            if entries:
                print(f"[DEBUG] {len(entries)} wpisów znaleziono w {filename}")
                new_entries.extend(entries)
                processed_files.add(filename)
            else:
                print(f"[DEBUG] Brak dopasowanych wpisów w {filename}")
        ftp.quit()
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania plików FTP: {e}")
    return new_entries

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        new_entries = fetch_log_files()
        if new_entries:
            print(f"[INFO] Znaleziono {len(new_entries)} nowych wpisów")
            save_to_db(new_entries)
            summary = generate_summary()
            try:
                requests.post(WEBHOOK_URL, json={"content": summary})
                print("[INFO] Wysłano dane na webhook")
            except Exception as e:
                print(f"[ERROR] Błąd wysyłania webhooka: {e}")
        else:
            print("[DEBUG] Brak nowych wpisów w logach")
        time.sleep(60)

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    threading.Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
