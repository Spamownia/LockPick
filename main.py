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

# =================== KONFIG ======================

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

CHECK_INTERVAL = 60  # sekundy

# =================== BAZA ======================

def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_stats (
            nick TEXT,
            castle TEXT,
            success BOOLEAN,
            duration INTEGER,
            PRIMARY KEY (nick, castle, success, duration)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_log_entry(nick, castle, success, duration):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO lockpicking_stats (nick, castle, success, duration)
        VALUES (%s, %s, %s, %s)
    """, (nick, castle, success, duration))
    conn.commit()
    cur.close()
    conn.close()

def fetch_all_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT nick, castle, success, duration FROM lockpicking_stats")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# =================== FTP ======================

def list_log_files(ftp):
    lines = []
    ftp.retrlines('LIST ' + FTP_LOG_PATH, lines.append)
    filenames = [line.split()[-1] for line in lines if line.split()[-1].startswith("gameplay_") and line.split()[-1].endswith(".log")]
    return filenames

def fetch_log_files():
    print("[DEBUG] Połączenie z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print("[INFO] Zalogowano do FTP")

    ftp.cwd(FTP_LOG_PATH)
    files = list_log_files(ftp)
    print(f"[DEBUG] Znaleziono {len(files)} plików logów.")

    log_entries = []

    for filename in files:
        print(f"[INFO] Przetwarzanie pliku: {filename}")
        bio = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        bio.seek(0)
        try:
            text = codecs.decode(bio.read(), 'utf-16-le')
        except Exception as e:
            print(f"[ERROR] Nie udało się odczytać pliku {filename}: {e}")
            continue

        entries = parse_log_text(text)
        log_entries.extend(entries)

    ftp.quit()
    return log_entries

# =================== PARSER ======================

def parse_log_text(text):
    pattern = r'LOCKPICK: ([\w\d_]+) attacked ([\w\d_]+), result: (SUCCESS|FAIL), duration: (\d+)ms'
    matches = re.findall(pattern, text)
    results = []
    for nick, castle, result, duration in matches:
        success = True if result == "SUCCESS" else False
        duration = int(duration)
        results.append((nick, castle, success, duration))
    print(f"[DEBUG] Wyodrębniono {len(results)} wpisów.")
    return results

# =================== WEBHOOK ======================

def send_stats_to_webhook():
    print("[INFO] Generowanie statystyk...")
    data = fetch_all_data()

    grouped = {}
    for nick, castle, success, duration in data:
        key = (nick, castle)
        if key not in grouped:
            grouped[key] = {
                "total": 0,
                "success": 0,
                "fail": 0,
                "durations": []
            }
        grouped[key]["total"] += 1
        if success:
            grouped[key]["success"] += 1
        else:
            grouped[key]["fail"] += 1
        grouped[key]["durations"].append(duration)

    header = "| Nick | Zamek | Ilość prób | Udane | Nieudane | Skuteczność | Średni czas |\n"
    separator = "|:----:|:-----:|:----------:|:-----:|:--------:|:------------:|:------------:|\n"
    rows = ""

    for (nick, castle), stats in grouped.items():
        total = stats["total"]
        succ = stats["success"]
        fail = stats["fail"]
        effectiveness = f"{(succ / total) * 100:.1f}%" if total > 0 else "0%"
        avg_time = f"{sum(stats['durations']) // len(stats['durations'])}ms" if stats['durations'] else "0ms"

        rows += f"| {nick} | {castle} | {total} | {succ} | {fail} | {effectiveness} | {avg_time} |\n"

    content = header + separator + rows
    requests.post(WEBHOOK_URL, json={"content": f"```\n{content}\n```"})

# =================== MAIN LOOP ======================

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    fetched_once = False

    while True:
        new_entries = fetch_log_files()

        if new_entries:
            print(f"[INFO] Nowe wpisy: {len(new_entries)}")
            for entry in new_entries:
                insert_log_entry(*entry)
            send_stats_to_webhook()
            fetched_once = True
        elif not fetched_once:
            print("[INFO] Brak nowych wpisów - początkowe sprawdzenie.")
        else:
            print("[INFO] Brak nowych wpisów.")

        time.sleep(CHECK_INTERVAL)

# =================== FLASK ======================

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

threading.Thread(target=main_loop).start()
app.run(host='0.0.0.0', port=3000)
