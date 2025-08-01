import os
import re
import time
import pandas as pd
import psycopg2
from flask import Flask
from tabulate import tabulate
from io import BytesIO
from ftplib import FTP
import requests

# Flask endpoint do pingowania
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# Dane logowania FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOGS_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# Webhook do Discorda
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Konfiguracja bazy danych PostgreSQL
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def initialize_database():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gameplay_logs (
            id SERIAL PRIMARY KEY,
            username TEXT,
            lock_type TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            UNIQUE(username, lock_type, success, elapsed_time)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def download_logs_from_ftp():
    print("[DEBUG] Łączenie z FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOGS_PATH)

    filenames = []
    ftp.retrlines('LIST', lambda x: filenames.append(x.split()[-1]))
    log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików gameplay_*.log")

    logs = []
    for filename in log_files:
        try:
            print(f"[DEBUG] Pobieranie {filename}...")
            bio = BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode('utf-16-le', errors='ignore')
            logs.append(content)
        except Exception as e:
            print(f"[ERROR] Nie udało się pobrać {filename}: {e}")

    ftp.quit()
    return logs

def parse_log_content(content):
    pattern = re.compile(
        r'\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<user>\w+).*?'
        r'Lock: (?P<lock>.+?)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[0-9.]+)',
        re.DOTALL
    )
    entries = pattern.findall(content)
    print(f"[DEBUG] Rozpoznano {len(entries)} wpisów lockpickingu w logu")
    data = []

    for user, lock, success, time_val in entries:
        data.append({
            "username": user,
            "lock_type": lock.strip(),
            "success": success == "Yes",
            "elapsed_time": float(time_val)
        })
    return data

def save_to_db(entries):
    conn = connect_db()
    cur = conn.cursor()
    inserted = 0
    for entry in entries:
        try:
            cur.execute("""
                INSERT INTO gameplay_logs (username, lock_type, success, elapsed_time)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (entry["username"], entry["lock_type"], entry["success"], entry["elapsed_time"]))
            inserted += cur.rowcount
        except Exception as e:
            print(f"[ERROR] Błąd zapisu do bazy: {e}")
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Zapisano {inserted} nowych wpisów do bazy")

def create_dataframe():
    conn = connect_db()
    df = pd.read_sql_query("SELECT * FROM gameplay_logs", conn)
    conn.close()

    if df.empty:
        print("[DEBUG] Brak danych w bazie.")
        return None

    grouped = df.groupby(["username", "lock_type"])
    stats = grouped.agg(
        Total_Attempts=pd.NamedAgg(column="success", aggfunc="count"),
        Successes=pd.NamedAgg(column="success", aggfunc="sum"),
        Failures=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        Accuracy=pd.NamedAgg(column="success", aggfunc=lambda x: round(100 * x.sum() / len(x), 1)),
        Avg_Time=pd.NamedAgg(column="elapsed_time", aggfunc="mean")
    ).reset_index()

    stats["Avg_Time"] = stats["Avg_Time"].round(2)

    return stats

def send_to_discord(df):
    if df is None:
        print("[DEBUG] Brak danych do wysłania.")
        return

    table = tabulate(df, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="github", stralign="center", numalign="center")
    print("[DEBUG] Tabela:\n" + table)

    payload = {
        "content": f"```\n{table}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Wysłano do Discorda: {response.status_code}")

def main_loop():
    print("[DEBUG] Start main_loop")
    initialize_database()

    while True:
        try:
            logs = download_logs_from_ftp()
            all_entries = []

            for content in logs:
                parsed = parse_log_content(content)
                all_entries.extend(parsed)

            if all_entries:
                print(f"[DEBUG] Łącznie {len(all_entries)} wpisów do zapisania")
                save_to_db(all_entries)
                df = create_dataframe()
                send_to_discord(df)
            else:
                print("[DEBUG] Brak nowych wpisów")

        except Exception as e:
            print(f"[ERROR] Błąd głównej pętli: {e}")

        time.sleep(60)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host='0.0.0.0', port=3000)
