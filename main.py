import os
import time
import threading
from ftplib import FTP
from io import BytesIO
from datetime import datetime

import pandas as pd
import psycopg2
from flask import Flask
from tabulate import tabulate
import requests

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- FLASK (PING UPTIMEROBOT) ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"

# --- BAZA DANYCH ---
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            id SERIAL PRIMARY KEY,
            nickname TEXT,
            lock_type TEXT,
            success BOOLEAN,
            elapsed FLOAT,
            timestamp TIMESTAMPTZ
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()

def insert_entries(entries):
    if not entries:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for e in entries:
        cur.execute('''
            INSERT INTO lockpick_logs (nickname, lock_type, success, elapsed, timestamp)
            VALUES (%s, %s, %s, %s, %s);
        ''', (e["nickname"], e["lock_type"], e["success"], e["elapsed"], e["timestamp"]))
    conn.commit()
    cur.close()
    conn.close()

# --- PRZETWARZANIE LOGÓW ---
def fetch_log_files():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)
    files = []
    ftp.retrlines("LIST", files.append)
    filenames = [line.split()[-1] for line in files if line.endswith(".log") and "gameplay_" in line]
    print(f"[DEBUG] Znaleziono {len(filenames)} plików logów.")
    logs = {}
    for fname in filenames:
        bio = BytesIO()
        try:
            ftp.retrbinary(f"RETR {fname}", bio.write)
            logs[fname] = bio.getvalue().decode("utf-16-le", errors="ignore")
            print(f"[DEBUG] Pobrano i zdekodowano: {fname}")
        except Exception as e:
            print(f"[ERROR] Nie można pobrać {fname}: {e}")
    ftp.quit()
    return logs

def parse_log_content(content):
    entries = []
    for line in content.splitlines():
        if "[LogMinigame]" in line and "User:" in line:
            try:
                timestamp_str = line.split(":", 1)[0]
                timestamp = datetime.strptime(timestamp_str.strip(), "%Y.%m.%d-%H.%M.%S")
                user = line.split("User:")[1].split("(")[0].strip()
                lock_type = line.split("Type:")[1].split(".")[0].strip()
                success = "Success: Yes" in line
                elapsed = line.split("Elapsed time:")[1].split()[0].strip().rstrip(".")
                elapsed = float(elapsed)
                entries.append({
                    "nickname": user,
                    "lock_type": lock_type,
                    "success": success,
                    "elapsed": elapsed,
                    "timestamp": timestamp
                })
            except Exception as e:
                print(f"[ERROR] Nie udało się sparsować linii: {line}\n{e}")
    print(f"[DEBUG] Rozpoznano {len(entries)} wpisów lockpick.")
    return entries

# --- TWORZENIE TABELI ---
def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM lockpick_logs;", conn)
    conn.close()

    if df.empty:
        return None

    df["result"] = df["success"].map({True: "Udane", False: "Nieudane"})
    grouped = df.groupby(["nickname", "lock_type"])

    summary = grouped.agg(
        proby=("success", "count"),
        udane=("success", "sum"),
        nieudane=("success", lambda x: (~x).sum()),
        skutecznosc=("success", lambda x: f"{(x.sum()/x.count())*100:.1f}%"),
        sredni_czas=("elapsed", "mean")
    ).reset_index()

    summary["sredni_czas"] = summary["sredni_czas"].map(lambda x: f"{x:.2f}s")
    summary = summary.rename(columns={
        "nickname": "Nick",
        "lock_type": "Zamek",
        "proby": "Ilość wszystkich prób",
        "udane": "Udane",
        "nieudane": "Nieudane",
        "skutecznosc": "Skuteczność",
        "sredni_czas": "Średni czas"
    })

    table = tabulate(summary, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    return table

# --- WYSYŁKA NA DISCORD ---
def send_to_discord(table):
    if not table:
        print("[DEBUG] Brak danych do wysłania.")
        return
    payload = {"content": f"```\n{table}\n```"}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            print("[DEBUG] Tabela wysłana na Discord.")
        else:
            print(f"[ERROR] Błąd wysyłki na Discord: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[ERROR] Wyjątek podczas wysyłki na Discord: {e}")

# --- PĘTLA GŁÓWNA ---
def main_loop():
    print("[DEBUG] Start main_loop")
    processed = set()
    while True:
        logs = fetch_log_files()
        new_entries = []
        for fname, content in logs.items():
            if fname not in processed:
                entries = parse_log_content(content)
                new_entries.extend(entries)
                processed.add(fname)
        if new_entries:
            print(f"[DEBUG] Nowych wpisów: {len(new_entries)}")
            insert_entries(new_entries)
            tabela = create_dataframe()
            send_to_discord(tabela)
        else:
            print("[DEBUG] Brak nowych danych do przetworzenia.")
        time.sleep(60)

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    init_db()
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
