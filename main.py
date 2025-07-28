import os
import re
import io
import time
import ftplib
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from flask import Flask
from datetime import datetime

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- FLASK ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"
    
# --- FUNKCJA PARSUJĄCA LINIE ---
def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame\].*?User: (?P<nick>.*?) \(\d+, (?P<steamid>\d+)\)\. Success: (?P<success>Yes|No).*?Elapsed time: (?P<time>\d+\.\d+).*?Target object: .*?\. Lock type: (?P<lock_type>\w+)",
        re.UNICODE
    )
    entries = []
    for match in pattern.finditer(content):
        data = match.groupdict()
        entries.append({
            "nick": data["nick"],
            "steamid": data["steamid"],
            "success": data["success"] == "Yes",
            "time": float(data["time"]),
            "lock_type": data["lock_type"]
        })
    return entries

# --- FUNKCJA ZAPISU DO DB ---
def save_to_database(entries):
    if not entries:
        print("[INFO] Brak danych do zapisania.")
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_stats (
                nick TEXT,
                steamid TEXT,
                lock_type TEXT,
                success BOOLEAN,
                time FLOAT,
                timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

        for entry in entries:
            cursor.execute("""
                INSERT INTO lockpicking_stats (nick, steamid, lock_type, success, time)
                VALUES (%s, %s, %s, %s, %s);
            """, (entry["nick"], entry["steamid"], entry["lock_type"], entry["success"], entry["time"]))
        conn.commit()
        print(f"[INFO] Zapisano {len(entries)} wpisów do bazy.")
    except Exception as e:
        print(f"[ERROR] Błąd zapisu do bazy: {e}")
    finally:
        cursor.close()
        conn.close()

# --- GENERUJ TABELĘ ---
def generate_table():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql("SELECT * FROM lockpicking_stats", conn)
        conn.close()
    except Exception as e:
        print(f"[ERROR] Błąd przy pobieraniu z bazy: {e}")
        return None

    if df.empty:
        return None

    df['success'] = df['success'].astype(bool)
    summary = df.groupby(["nick", "lock_type"]).agg(
        total_attempts=pd.NamedAgg(column="success", aggfunc="count"),
        successful=pd.NamedAgg(column="success", aggfunc="sum"),
        failed=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        effectiveness=pd.NamedAgg(column="success", aggfunc=lambda x: round(x.mean() * 100, 2)),
        avg_time=pd.NamedAgg(column="time", aggfunc="mean")
    ).reset_index()

    summary["avg_time"] = summary["avg_time"].round(2)
    summary["effectiveness"] = summary["effectiveness"].astype(str) + '%'

    table = tabulate(summary, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="grid", stralign="center")
    return table

# --- WYŚLIJ NA DISCORD ---
def send_to_discord(table):
    if not table:
        print("[INFO] Brak danych do wysłania.")
        return
    try:
        response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table}\n```"})
        if response.status_code == 204:
            print("[OK] Tabela wysłana na webhook.")
        else:
            print(f"[ERROR] Nie udało się wysłać: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Błąd podczas wysyłki webhooka: {e}")

# --- WŁAŚCIWA LOGIKA ---
def main_loop():
    print("[DEBUG] Start programu")
    downloaded = 0
    all_entries = []

    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")

        filenames = []
        ftp.retrlines('LIST', lambda line: filenames.append(line.split()[-1]))
        log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]

        for filename in sorted(log_files):
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode("utf-16-le", errors="ignore")
            entries = parse_log_content(content)
            if entries:
                all_entries.extend(entries)
            print(f"[INFO] Załadowano: {filename}")
            downloaded += 1
        ftp.quit()
    except Exception as e:
        print(f"[ERROR] Błąd FTP: {e}")
        return

    print(f"[DEBUG] Wszystkich wpisów: {len(all_entries)}")

    if all_entries:
        save_to_database(all_entries)
        table = generate_table()
        send_to_discord(table)
    else:
        print("[INFO] Brak nowych rozpoznanych wpisów.")

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    from threading import Thread
    t = Thread(target=main_loop)
    t.start()
    app.run(host="0.0.0.0", port=3000)
