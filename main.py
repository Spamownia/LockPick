import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
import threading
import requests
from tabulate import tabulate
from datetime import datetime
from flask import Flask

# === KONFIGURACJA ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# === INICJALIZACJA ===
app = Flask(__name__)
last_offset = 0

# === FLASK ===
@app.route('/')
def index():
    return "Alive"

# === FUNKCJE ===

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def parse_log_content(content):
    pattern = re.compile(r"\[LogMinigame\].*?User:\s*(?P<nick>.*?)\s+.*?Lock:\s*(?P<lock>.*?)\s+.*?Success:\s*(?P<success>Yes|No).*?Elapsed time:\s*(?P<time>[0-9.]+)", re.DOTALL)
    return pattern.findall(content)

def create_dataframe(data):
    stats = {}
    for nick, lock, success, time in data:
        key = (nick.strip(), lock.strip())
        if key not in stats:
            stats[key] = {
                "total": 0,
                "success": 0,
                "fail": 0,
                "times": []
            }
        stats[key]["total"] += 1
        if success == "Yes":
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1
        stats[key]["times"].append(float(time))

    rows = []
    for (nick, lock), s in stats.items():
        success_rate = round(100 * s["success"] / s["total"], 1)
        avg_time = round(sum(s["times"]) / len(s["times"]), 2)
        rows.append([nick, lock, s["total"], s["success"], s["fail"], f"{success_rate}%", avg_time])

    df = pd.DataFrame(rows, columns=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"])
    return df

def send_to_discord(df):
    if df.empty:
        print("[INFO] Brak danych do wysłania.")
        return

    table = tabulate(df.values.tolist(), headers=df.columns.tolist(), tablefmt="grid", stralign="center", numalign="center")
    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.ok:
        print("[OK] Wysłano dane do Discorda.")
    else:
        print(f"[ERROR] Błąd przy wysyłaniu do Discorda: {response.status_code} {response.text}")

def process_log_lines(lines):
    parsed_data = parse_log_content("".join(lines))
    if parsed_data:
        df = create_dataframe(parsed_data)
        send_to_discord(df)
    else:
        print("[INFO] Brak nowych rozpoznanych wpisów w logu.")

def check_new_log_entries():
    global last_offset

    try:
        ftp = connect_ftp()
        ftp.cwd(FTP_LOG_PATH)
        files = []
        ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
        log_files = sorted([f for f in files if f.startswith("gameplay_") and f.endswith(".log")])

        if not log_files:
            print("[INFO] Brak plików gameplay_*.log.")
            return

        latest_file = log_files[-1]
        print(f"[DEBUG] Najnowszy log: {latest_file}")
        lines = []

        ftp.retrbinary(f"RETR {latest_file}", lambda data: lines.append(data), blocksize=1024)
        raw = b''.join(lines).decode("utf-16le", errors="ignore")
        ftp.quit()

        log_lines = raw.splitlines()
        total_lines = len(log_lines)

        if last_offset < total_lines:
            new_lines = log_lines[last_offset:]
            print(f"[DEBUG] Nowe linie: {len(new_lines)}")
            process_log_lines(new_lines)
            last_offset = total_lines
        else:
            print("[INFO] Brak nowych wpisów w logu.")

    except Exception as e:
        print(f"[ERROR] {e}")

def loop():
    print("[DEBUG] Start main_loop")
    while True:
        check_new_log_entries()
        time.sleep(60)

# === START ===
if __name__ == "__main__":
    threading.Thread(target=loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
