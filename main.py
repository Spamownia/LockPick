import os import re import ftplib import psycopg2 import requests import threading import time import io import codecs from datetime import datetime from flask import Flask

Konfiguracja FTP

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

Konfiguracja webhooka

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

Konfiguracja bazy danych

DB_CONFIG = { "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", "dbname": "neondb", "user": "neondb_owner", "password": "npg_dRU1YCtxbh6v", "sslmode": "require" }

db_conn = None processed_files = set()

def init_db(): global db_conn db_conn = psycopg2.connect(**DB_CONFIG) cursor = db_conn.cursor() cursor.execute(""" CREATE TABLE IF NOT EXISTS lockpick_stats ( nick TEXT, castle TEXT, success BOOLEAN, duration FLOAT, timestamp TIMESTAMP ) """) db_conn.commit() cursor.close() print("[INFO] Inicjalizacja bazy danych...")

def parse_log_content(content): entries = [] pattern = re.compile(r"(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): \ Character '(.?)' started lockpicking on '(.?)'(.?)(Succeeded|Failed).?in ([\d.]+) seconds") for match in pattern.finditer(content): timestamp_str, thread, nick, castle, _, result, duration = match.groups() timestamp = datetime.strptime(timestamp_str, "%Y.%m.%d-%H.%M.%S") success = result == "Succeeded" duration = float(duration) entries.append((nick, castle, success, duration, timestamp)) return entries

def save_entries_to_db(entries): if not entries: return cursor = db_conn.cursor() for entry in entries: cursor.execute(""" INSERT INTO lockpick_stats (nick, castle, success, duration, timestamp) VALUES (%s, %s, %s, %s, %s) """, entry) db_conn.commit() cursor.close()

def summarize_data(): cursor = db_conn.cursor() cursor.execute(""" SELECT nick, castle, COUNT() AS total, SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count, SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS fail_count, ROUND(AVG(duration), 2) AS avg_time, ROUND(100.0 * SUM(CASE WHEN success THEN 1 ELSE 0 END) / COUNT(), 2) AS effectiveness FROM lockpick_stats GROUP BY nick, castle ORDER BY effectiveness DESC """) rows = cursor.fetchall() cursor.close() return rows

def send_to_webhook(summary): if not summary: return

headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
col_widths = [max(len(str(row[i])) for row in summary + [headers]) for i in range(len(headers))]

lines = [" | ".join(f"{headers[i]:^{col_widths[i]}}" for i in range(len(headers)))]
lines.append("-+-".join("-" * w for w in col_widths))
for row in summary:
    lines.append(" | ".join(f"{str(row[i]):^{col_widths[i]}}" for i in range(len(headers))))

content = "```

" + "\n".join(lines) + "

requests.post(WEBHOOK_URL, json={"content": content})

def fetch_log_files():
    new_entries = []
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOG_DIR)
            filenames = []
            ftp.retrlines('LIST', lambda line: filenames.append(line.split()[-1]))
            log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]

            for filename in log_files:
                if filename in processed_files:
                    continue
                print(f"[INFO] Przetwarzanie pliku: {filename}")
                r = io.BytesIO()
                ftp.retrbinary(f"RETR {filename}", r.write)
                content = r.getvalue().decode("utf-16-le", errors="ignore")
                entries = parse_log_content(content)
                if entries:
                    print(f"[DEBUG] {len(entries)} wpisów znaleziono w {filename}")
                    new_entries.extend(entries)
                    processed_files.add(filename)
                else:
                    print(f"[DEBUG] Brak wpisów w pliku: {filename}")
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania logów: {e}")
    return new_entries

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        new_entries = fetch_log_files()
        if new_entries:
            print(f"[INFO] Znaleziono {len(new_entries)} nowych wpisów")
            save_entries_to_db(new_entries)
            summary = summarize_data()
            send_to_webhook(summary)
        else:
            print("[DEBUG] Brak nowych wpisów w logach")
        time.sleep(60)

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

if __name__ == '__main__':
    threading.Thread(target=main_loop).start()
    app.run(host='0.0.0.0', port=3000)

