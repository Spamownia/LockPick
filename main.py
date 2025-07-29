import os
import re
import io
import time
import ftplib
import psycopg2
import pandas as pd
import datetime
import requests
from flask import Flask
from tabulate import tabulate

# === KONFIGURACJA ===

FTP_CONFIG = {
    "host": "176.57.174.10",
    "port": 50021,
    "user": "gpftp37275281717442833",
    "passwd": "LXNdGShY"
}

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
LOGS_DIR = "/SCUM/Saved/SaveFiles/Logs/"
LOG_PATTERN = re.compile(r'^\[LogMinigame] \[LockpickingMinigame_C] User: (?P<nick>.*?) \((?P<id>.*?)\) Lock: (?P<lock>.*?) Success: (?P<success>Yes|No).+?Elapsed time: (?P<time>[0-9.]+)', re.MULTILINE)

# === FLASK (ping) ===
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# === FUNKCJE ===

def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def init_db():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_results (
            nick TEXT,
            lock TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DEBUG] Baza danych zainicjalizowana.")

def fetch_log_files():
    print("[DEBUG] Łączenie z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_CONFIG["host"], FTP_CONFIG["port"])
    ftp.login(FTP_CONFIG["user"], FTP_CONFIG["passwd"])
    ftp.cwd(LOGS_DIR)

    files = []
    ftp.retrlines('LIST', lambda line: files.append(line.split()[-1]))
    gameplay_logs = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono pliki logów: {gameplay_logs}")

    logs = {}
    for filename in gameplay_logs:
        bio = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        content = bio.getvalue().decode("utf-16le", errors="ignore")
        logs[filename] = content
        print(f"[DEBUG] Pobrano: {filename} ({len(content)} znaków)")
    ftp.quit()
    return logs

def parse_log_content(log_text):
    entries = []
    for match in LOG_PATTERN.finditer(log_text):
        data = match.groupdict()
        entries.append({
            "nick": data["nick"],
            "lock": data["lock"],
            "success": data["success"] == "Yes",
            "elapsed_time": float(data["time"])
        })
    print(f"[DEBUG] Przetworzono {len(entries)} wpisów z logu.")
    return entries

def load_existing_entries():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT nick, lock, success, elapsed_time FROM lockpicking_results")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return set(tuple(row) for row in rows)

def save_new_entries(new_entries):
    conn = connect_db()
    cur = conn.cursor()
    for entry in new_entries:
        cur.execute("""
            INSERT INTO lockpicking_results (nick, lock, success, elapsed_time)
            VALUES (%s, %s, %s, %s)
        """, (entry["nick"], entry["lock"], entry["success"], entry["elapsed_time"]))
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Zapisano {len(new_entries)} nowych wpisów do bazy.")

def create_dataframe(entries):
    df = pd.DataFrame(entries)
    if df.empty:
        return None
    grouped = df.groupby(["nick", "lock"]).agg(
        Attempts=("success", "count"),
        Successes=("success", "sum"),
        Failures=("success", lambda x: (~x).sum()),
        Efficiency=("success", "mean"),
        AvgTime=("elapsed_time", "mean")
    ).reset_index()

    grouped["Efficiency"] = (grouped["Efficiency"] * 100).round(1).astype(str) + "%"
    grouped["AvgTime"] = grouped["AvgTime"].round(2).astype(str) + "s"

    table = tabulate(
        grouped,
        headers=["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"],
        tablefmt="github",
        showindex=False,
        stralign="center",
        numalign="center"
    )
    return table

def send_to_discord(message):
    if message:
        data = {"content": f"```\n{message}\n```"}
        response = requests.post(WEBHOOK_URL, json=data)
        if response.status_code == 204:
            print("[DEBUG] Wysłano tabelę do Discorda.")
        else:
            print(f"[ERROR] Nie udało się wysłać: {response.status_code}")
    else:
        print("[DEBUG] Brak danych do wysłania.")

def main_loop():
    init_db()
    last_known_entries = load_existing_entries()
    print("[DEBUG] Start main_loop")

    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.datetime.now(datetime.timezone.utc).isoformat()} ---")
        try:
            logs = fetch_log_files()
            all_entries = []

            for filename, content in logs.items():
                parsed = parse_log_content(content)
                all_entries.extend(parsed)

            current_entries_set = set((e["nick"], e["lock"], e["success"], e["elapsed_time"]) for e in all_entries)
            new_entries = [e for e in all_entries if (e["nick"], e["lock"], e["success"], e["elapsed_time"]) not in last_known_entries]

            if new_entries:
                print(f"[DEBUG] Wykryto {len(new_entries)} nowych wpisów.")
                save_new_entries(new_entries)
                last_known_entries.update(current_entries_set)
                table = create_dataframe(all_entries)
                send_to_discord(table)
            else:
                print("[DEBUG] Brak nowych wpisów w logach.")

        except Exception as e:
            print(f"[ERROR] Wystąpił błąd w pętli głównej: {e}")

        time.sleep(60)

# === START APLIKACJI ===
if __name__ == "__main__":
    from threading import Thread
    thread = Thread(target=main_loop)
    thread.daemon = True
    thread.start()

    app.run(host="0.0.0.0", port=3000)
