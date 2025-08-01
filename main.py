import os import time import ftplib import psycopg2 import pandas as pd from tabulate import tabulate from io import BytesIO from flask import Flask

Konfiguracja FTP i DB

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = { "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", "dbname": "neondb", "user": "neondb_owner", "password": "npg_dRU1YCtxbh6v", "sslmode": "require" }

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(name)

@app.route("/") def index(): return "Alive"

def connect_ftp(): ftp = ftplib.FTP() ftp.connect(FTP_HOST, FTP_PORT) ftp.login(FTP_USER, FTP_PASS) ftp.cwd(FTP_DIR) print("[DEBUG] Połączono z FTP i ustawiono katalog") return ftp

def init_db(): conn = psycopg2.connect(**DB_CONFIG) with conn.cursor() as cur: cur.execute(""" CREATE TABLE IF NOT EXISTS lockpick_logs ( id SERIAL PRIMARY KEY, nickname TEXT, lock_type TEXT, success BOOLEAN, elapsed_time FLOAT, timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP ) """) conn.commit() conn.close() print("[DEBUG] Zainicjalizowano bazę danych")

def parse_log_content(content): lines = content.splitlines() entries = [] for line in lines: if "[LogMinigame] [LockpickingMinigame_C]" in line and "Success:" in line: try: nickname = line.split("User: ")[1].split(" ")[0] lock_type = line.split("Type: ")[1].split(" ")[0] success = "Success: Yes" in line elapsed = float(line.split("Elapsed time: ")[1].split(" ")[0]) entries.append((nickname, lock_type, success, elapsed)) except Exception as e: print(f"[DEBUG] Błąd parsowania linii: {line}\n{e}") print(f"[DEBUG] Rozpoznano {len(entries)} wpisów w logu") return entries

def fetch_and_process_logs(): ftp = connect_ftp() filenames = [] ftp.retrlines("LIST", lambda x: filenames.append(x.split()[-1]) if "gameplay_" in x else None) print(f"[DEBUG] Znalezione pliki: {filenames}")

conn = psycopg2.connect(**DB_CONFIG)
inserted = 0
for filename in filenames:
    memory_file = BytesIO()
    try:
        ftp.retrbinary(f"RETR {filename}", memory_file.write)
        content = memory_file.getvalue().decode("utf-16-le")
        entries = parse_log_content(content)
        with conn.cursor() as cur:
            for nick, lock, succ, time_ in entries:
                cur.execute("""
                    INSERT INTO lockpick_logs (nickname, lock_type, success, elapsed_time)
                    VALUES (%s, %s, %s, %s)
                """, (nick, lock, succ, time_))
                inserted += 1
        conn.commit()
    except Exception as e:
        print(f"[DEBUG] Błąd przetwarzania pliku {filename}: {e}")
conn.close()
ftp.quit()
print(f"[DEBUG] Wstawiono {inserted} nowych wpisów do bazy")
return inserted > 0

def create_dataframe(): conn = psycopg2.connect(**DB_CONFIG) df = pd.read_sql_query("SELECT * FROM lockpick_logs", conn) conn.close() if df.empty: print("[DEBUG] Brak danych w tabeli") return None grouped = df.groupby(["nickname", "lock_type"]) result = grouped.agg( all_attempts=pd.NamedAgg(column="success", aggfunc="count"), successes=pd.NamedAgg(column="success", aggfunc="sum"), avg_time=pd.NamedAgg(column="elapsed_time", aggfunc="mean") ) result["failures"] = result["all_attempts"] - result["successes"] result["efficiency"] = (result["successes"] / result["all_attempts"] * 100).round(1) result = result.reset_index() print("[DEBUG] Utworzono tabelę wyników") return result

def send_to_discord(df): headers = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"] rows = [] for _, row in df.iterrows(): rows.append([ row["nickname"], row["lock_type"], row["all_attempts"], row["successes"], row["failures"], f"{row['efficiency']}%", f"{row['avg_time']:.2f}s" ]) table = tabulate(rows, headers=headers, tablefmt="grid", stralign="center", numalign="center") print("[DEBUG] Wysyłam tabelę na Discorda") import requests requests.post(WEBHOOK_URL, json={"content": f"\n{table}\n"})

def main_loop(): print("[DEBUG] Start main_loop") init_db() while True: print("[DEBUG] Iteracja pętli") try: updated = fetch_and_process_logs() if updated: df = create_dataframe() if df is not None: send_to_discord(df) else: print("[DEBUG] Brak nowych wpisów w logach") except Exception as e: print(f"[DEBUG] Błąd w main_loop: {e}") time.sleep(60)

if name == "main": import threading threading.Thread(target=main_loop).start() app.run(host="0.0.0.
0", port=3000)
