import os
import ftplib
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

# === INICJALIZACJA FLASK ===

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# === OBSŁUGA BAZY DANYCH ===

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            zamek TEXT,
            powodzenie BOOLEAN,
            czas REAL,
            timestamp TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

# === POBIERANIE LOGÓW ===

def fetch_log_files():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)

        print("[INFO] Połączono z FTP i zmieniono katalog.")

        files = []
        try:
            files = ftp.nlst()
        except ftplib.error_perm as e:
            if "502" in str(e):
                print("[WARN] Serwer nie obsługuje NLST – próbuję FTP.dir()")
                buffer = []
                ftp.dir(buffer.append)
                files = [line.split()[-1] for line in buffer if "gameplay_" in line]
            else:
                raise

        log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
        print(f"[INFO] Znaleziono {len(log_files)} plików logów.")

        logs = []
        for file_name in log_files:
            with io.BytesIO() as bio:
                ftp.retrbinary(f"RETR {file_name}", bio.write)
                bio.seek(0)
                decoded = bio.read().decode('utf-16-le', errors='ignore')
                logs.append(decoded)
                print(f"[DEBUG] Pobrano i zdekodowano plik: {file_name}")

        ftp.quit()
        return logs
    except Exception as e:
        print(f"[ERROR] Błąd FTP: {e}")
        return []

# === PARSOWANIE LOGÓW ===

def parse_logs(logs):
    entries = []
    pattern = re.compile(
        r'\[(?P<timestamp>[\d\-\.\s:]+)\].*?Player (?P<nick>\w+).*?tried to pick lock on (?P<zamek>.*?) and (?P<result>succeeded|failed) in (?P<czas>[\d\.]+)s'
    )
    for content in logs:
        for match in pattern.finditer(content):
            data = match.groupdict()
            entries.append({
                'nick': data['nick'],
                'zamek': data['zamek'],
                'powodzenie': data['result'] == 'succeeded',
                'czas': float(data['czas']),
                'timestamp': datetime.strptime(data['timestamp'], "%Y.%m.%d-%H.%M.%S")
            })
    print(f"[INFO] Sparsowano {len(entries)} wpisów z logów.")
    return entries

# === ZAPIS DO BAZY ===

def save_to_db(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_entries = 0
    for e in entries:
        cur.execute("""
            SELECT 1 FROM lockpick_stats
            WHERE nick = %s AND zamek = %s AND timestamp = %s
        """, (e['nick'], e['zamek'], e['timestamp']))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO lockpick_stats (nick, zamek, powodzenie, czas, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (e['nick'], e['zamek'], e['powodzenie'], e['czas'], e['timestamp']))
            new_entries += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"[INFO] Dodano {new_entries} nowych wpisów.")
    return new_entries > 0

# === STATYSTYKI ===

def generate_stats():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, zamek,
               COUNT(*) as total,
               COUNT(*) FILTER (WHERE powodzenie) as success,
               COUNT(*) FILTER (WHERE NOT powodzenie) as fail,
               ROUND(100.0 * COUNT(*) FILTER (WHERE powodzenie) / COUNT(*), 1) as skutecznosc,
               ROUND(AVG(czas), 2) as avg_time
        FROM lockpick_stats
        GROUP BY nick, zamek
        ORDER BY skutecznosc DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    header = "| Nick | Zamek | Ilość prób | Udane | Nieudane | Skuteczność | Średni czas |\n"
    header += "|:----:|:-----:|:-----------:|:-----:|:--------:|:-----------:|:-----------:|\n"
    lines = [
        f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]}% | {r[6]}s |"
        for r in rows
    ]
    return header + "\n".join(lines)

# === WEBHOOK ===

def send_to_discord(message):
    data = {"content": f"```markdown\n{message}\n```"}
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code == 204:
        print("[INFO] Wysłano dane do Discord.")
    else:
        print(f"[WARN] Błąd webhooka: {response.status_code}")

# === GŁÓWNA PĘTLA ===

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        logs = fetch_log_files()
        if logs:
            parsed = parse_logs(logs)
            if parsed:
                updated = save_to_db(parsed)
                if updated:
                    stats = generate_stats()
                    send_to_discord(stats)
                else:
                    print("[INFO] Brak nowych wpisów.")
            else:
                print("[INFO] Brak danych do przetworzenia.")
        else:
            print("[INFO] Brak logów do pobrania.")
        time.sleep(60)

# === START APLIKACJI ===

if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
