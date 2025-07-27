import os import re import psycopg2 import requests import threading from flask import Flask from datetime import datetime from ftplib import FTP_TLS from io import BytesIO

Konfiguracja FTP

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY"

Konfiguracja webhooka Discord

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

Konfiguracja bazy danych

DB_CONFIG = { 'host': "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", 'dbname': "neondb", 'user': "neondb_owner", 'password': "npg_dRU1YCtxbh6v", 'sslmode': "require" }

LOGS_DIR = "/SCUM/Saved/SaveFiles/Logs/"

app = Flask(name)

def connect_db(): return psycopg2.connect(**DB_CONFIG)

def parse_log_content(content): # Zwraca listę dopasowanych wpisów lockpicków entries = [] lines = content.splitlines() for line in lines: if "[LockPicking]" in line: match = re.search(r"      ", line) if match: date = match.group(1) nick = match.group(2) castle = match.group(3) result = match.group(4) time = match.group(5) method = match.group(6) entries.append((date, nick, castle, result, time, method)) return entries

def save_entries_to_db(entries): conn = connect_db() cur = conn.cursor() for entry in entries: cur.execute(''' INSERT INTO lockpicks (event_time, nick, castle, result, duration, method) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING; ''', entry) conn.commit() cur.close() conn.close()

def fetch_and_process_logs(): print("[DEBUG] Nawiązywanie połączenia FTP...") ftps = FTP_TLS() ftps.connect(FTP_HOST, FTP_PORT) ftps.login(FTP_USER, FTP_PASS) ftps.prot_p() ftps.cwd(LOGS_DIR)

filenames = []
ftps.retrlines('LIST', lambda line: filenames.append(line.split()[-1]))

gameplay_logs = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]

print(f"[DEBUG] Znaleziono {len(gameplay_logs)} plików gameplay_*.log")
total_entries = []

for filename in gameplay_logs:
    print(f"[DEBUG] Przetwarzanie: {filename}")
    bio = BytesIO()
    ftps.retrbinary(f"RETR {filename}", bio.write)
    content = bio.getvalue().decode("utf-16-le", errors="ignore")
    entries = parse_log_content(content)
    total_entries.extend(entries)

if total_entries:
    print(f"[DEBUG] Zapis {len(total_entries)} rekordów do bazy danych...")
    save_entries_to_db(total_entries)
else:
    print("[DEBUG] Brak nowych wpisów do zapisania.")

ftps.quit()

def start_main(): print("[DEBUG] Start main_loop") fetch_and_process_logs()

@app.route('/') def index(): return "Alive"

if name == 'main': threading.Thread(target=start_main).start() app.run(host='0.0.0.0', port=3000)

