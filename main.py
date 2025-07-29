import os import re import time import ftplib import psycopg2 import pandas as pd from tabulate import tabulate from datetime import datetime from flask import Flask import requests

Konfiguracja FTP i DB

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = { "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", "dbname": "neondb", "user": "neondb_owner", "password": "npg_dRU1YCtxbh6v", "sslmode": "require", }

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(name)

@app.route('/') def index(): return "Alive"

def connect_db(): return psycopg2.connect(**DB_CONFIG)

def initialize_db(): with connect_db() as conn: with conn.cursor() as cur: cur.execute(""" CREATE TABLE IF NOT EXISTS lockpicking_stats ( id SERIAL PRIMARY KEY, nick TEXT, lock_type TEXT, success BOOLEAN, elapsed_time FLOAT, timestamp TIMESTAMP ) """) conn.commit() print("[DEBUG] Baza danych gotowa.")

def list_log_files(ftp): ftp.cwd(FTP_DIR) files = [] ftp.retrlines("LIST", lambda line: files.append(line.split()[-1])) return sorted([f for f in files if f.startswith("gameplay_") and f.endswith(".log")])

def download_log_file(ftp, filename): from io import BytesIO bio = BytesIO() ftp.retrbinary(f"RETR {filename}", bio.write) bio.seek(0) return bio.read().decode("utf-16le", errors="ignore")

def parse_log_content(content): entries = [] pattern = re.compile(r"\[LogMinigame] \[LockpickingMinigame_C] User: (.?) .?Type: (.?) .?Success: (Yes|No).*?Elapsed time: ([\d.]+)") for match in pattern.finditer(content): nick, lock_type, success, elapsed = match.groups() entries.append({ "nick": nick.strip(), "lock_type": lock_type.strip(), "success": success == "Yes", "elapsed_time": float(elapsed), "timestamp": datetime.utcnow() }) return entries

def insert_new_entries(entries): new_count = 0 if not entries: return 0 with connect_db() as conn: with conn.cursor() as cur: for entry in entries: cur.execute(""" SELECT 1 FROM lockpicking_stats WHERE nick = %s AND lock_type = %s AND success = %s AND elapsed_time = %s ORDER BY id DESC LIMIT 1 """, (entry["nick"], entry["lock_type"], entry["success"], entry["elapsed_time"])) if cur.fetchone() is None: cur.execute(""" INSERT INTO lockpicking_stats (nick, lock_type, success, elapsed_time, timestamp) VALUES (%s, %s, %s, %s, %s) """, (entry["nick"], entry["lock_type"], entry["success"], entry["elapsed_time"], entry["timestamp"])) new_count += 1 conn.commit() return new_count

def create_dataframe(): with connect_db() as conn: df = pd.read_sql_query("SELECT * FROM lockpicking_stats", conn) if df.empty: return "Brak danych." grouped = df.groupby(["nick", "lock_type"]).agg( total_attempts=pd.NamedAgg(column="success", aggfunc="count"), successes=pd.NamedAgg(column="success", aggfunc="sum"), failures=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()), effectiveness=pd.NamedAgg(column="success", aggfunc="mean"), avg_time=pd.NamedAgg(column="elapsed_time", aggfunc="mean") ).reset_index() grouped["effectiveness"] = (grouped["effectiveness"] * 100).round(1).astype(str) + "%" grouped["avg_time"] = grouped["avg_time"].round(2) return tabulate(grouped, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="github", stralign="center", numalign="center")

def send_to_discord(table_text): payload = {"content": f"``` {table_text}

response = requests.post(WEBHOOK_URL, json=payload)
    print("[DEBUG] Webhook status:", response.status_code)

def main_loop():
    print("[DEBUG] Start main_loop")
    last_log = ""
    last_content = ""
    while True:
        try:
            print("[DEBUG] Iteracja pętli o", datetime.utcnow())
            with ftplib.FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT)
                ftp.login(FTP_USER, FTP_PASS)
                log_files = list_log_files(ftp)
                if not log_files:
                    print("[DEBUG] Brak plików gameplay_*.log")
                    time.sleep(60)
                    continue

                newest_log = log_files[-1]
                if newest_log != last_log:
                    print(f"[DEBUG] Nowy plik logu: {newest_log}")
                    last_log = newest_log
                    last_content = ""

                content = download_log_file(ftp, newest_log)
                if content == last_content:
                    print("[DEBUG] Brak nowych wpisów w logu.")
                    time.sleep(60)
                    continue

                entries = parse_log_content(content)
                new_count = insert_new_entries(entries)
                if new_count:
                    print(f"[DEBUG] Dodano {new_count} nowych wpisów.")
                    table = create_dataframe()
                    send_to_discord(table)
                else:
                    print("[DEBUG] Nie znaleziono nowych wpisów do dodania.")

                last_content = content
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(60)

if __name__ == "__main__":
    initialize_db()
    main_loop()
    app.run(host='
    0.0.0.0', port=3000)
