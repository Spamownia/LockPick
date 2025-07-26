import ftplib import os import re import psycopg2 import requests import threading import time import io import codecs from datetime import datetime from flask import Flask

Konfiguracja FTP i webhook

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/" WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

Konfiguracja bazy danych

DB_CONFIG = { 'host': "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", 'dbname': "neondb", 'user': "neondb_owner", 'password': "npg_dRU1YCtxbh6v", 'sslmode': "require" }

Globalny znacznik przetworzonych plików

processed_files = set()

app = Flask(name)

def connect_ftp(): ftp = ftplib.FTP() ftp.connect(FTP_HOST, FTP_PORT) ftp.login(FTP_USER, FTP_PASS) ftp.cwd(FTP_DIR) return ftp

def init_db(): conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() cur.execute(''' CREATE TABLE IF NOT EXISTS lockpicking ( nick TEXT, castle TEXT, result TEXT, duration FLOAT, timestamp TIMESTAMP ) ''') conn.commit() cur.close() conn.close()

Funkcja do wczytania i sparsowania danych z loga

log_entry_pattern = re.compile(r"(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}).?Character '(.?)'.?performed lockpicking on (.?) and (succeeded|failed) in (\d+(?:\.\d+)?) seconds")

def parse_log_content(content): entries = [] for match in log_entry_pattern.finditer(content): timestamp_str, nick, castle, result, duration = match.groups() timestamp = datetime.strptime(timestamp_str, "%Y.%m.%d-%H.%M.%S") entries.append((nick, castle, result, float(duration), timestamp)) return entries

def save_entries_to_db(entries): conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() new_count = 0 for entry in entries: cur.execute(''' SELECT 1 FROM lockpicking WHERE nick = %s AND castle = %s AND result = %s AND duration = %s AND timestamp = %s ''', entry) if not cur.fetchone(): cur.execute(''' INSERT INTO lockpicking (nick, castle, result, duration, timestamp) VALUES (%s, %s, %s, %s, %s) ''', entry) new_count += 1 conn.commit() cur.close() conn.close() return new_count

def fetch_log_files(): ftp = connect_ftp() files = [] ftp.retrlines('LIST', files.append) new_entries = [] for file_line in files: parts = file_line.split() if len(parts) < 9: continue filename = parts[-1] if not filename.startswith("gameplay_") or not filename.endswith(".log"): continue if filename in processed_files: continue print(f"[DEBUG] Przetwarzanie pliku: {filename}") with io.BytesIO() as f: ftp.retrbinary(f'RETR {filename}', f.write) f.seek(0) content = f.read().decode('utf-16-le') entries = parse_log_content(content) new_entries.extend(entries) processed_files.add(filename) ftp.quit() return new_entries

def format_stats(): conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() cur.execute("SELECT nick, castle, result, duration FROM lockpicking") rows = cur.fetchall() cur.close() conn.close()

stats = {}
for nick, castle, result, duration in rows:
    key = (nick, castle)
    if key not in stats:
        stats[key] = {"total": 0, "success": 0, "fail": 0, "times": []}
    stats[key]["total"] += 1
    stats[key]["times"].append(duration)
    if result == "succeeded":
        stats[key]["success"] += 1
    else:
        stats[key]["fail"] += 1

lines = ["| Nick | Zamek | Ilość wszystkich prób | Udane | Nieudane | Skuteczność | Średni czas |",
         "|:----:|:-----:|:---------------------:|:-----:|:--------:|:-----------:|:-----------:|"]

for (nick, castle), data in stats.items():
    total = data["total"]
    success = data["success"]
    fail = data["fail"]
    effectiveness = f"{(success / total * 100):.1f}%"
    avg_time = f"{(sum(data['times']) / len(data['times'])):.2f}s"
    lines.append(f"| {nick} | {castle} | {total} | {success} | {fail} | {effectiveness} | {avg_time} |")

return "\n".join(lines)

def send_to_webhook(message): requests.post(WEBHOOK_URL, json={"content": f"markdown\n{message}\n"})

def main_loop(): print("[DEBUG] Start main_loop") print("[INFO] Inicjalizacja bazy danych...") init_db() entries = fetch_log_files() if entries: print(f"[INFO] Znaleziono {len(entries)} wpisów, zapisuję do bazy...") count = save_entries_to_db(entries) print(f"[INFO] Zapisano {count} nowych wpisów.") stats = format_stats() send_to_webhook(stats) else: print("[INFO] Brak nowych danych.") while True: time.sleep(60) entries = fetch_log_files() if entries: print(f"[INFO] Znaleziono {len(entries)} wpisów, zapisuję do bazy...") count = save_entries_to_db(entries) print(f"[INFO] Zapisano {count} nowych wpisów.") stats = format_stats() send_to_webhook(stats) else: print("[INFO] Brak nowych danych.")

@app.route('/') def index(): return "Alive"

if name == 'main': threading.Thread(target=main_loop, daemon=True).start() app.run(host='0.0.0.0', port=3000)

