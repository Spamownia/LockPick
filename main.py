import os import re import io import time import ssl import psycopg2 import requests from ftplib import FTP from flask import Flask from datetime import datetime from collections import defaultdict

Konfiguracja

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/" WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = { "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", "dbname": "neondb", "user": "neondb_owner", "password": "npg_dRU1YCtxbh6v", "sslmode": "require" }

def init_db(): print("[DEBUG] Inicjalizacja bazy danych...") conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() cur.execute(""" CREATE TABLE IF NOT EXISTS lockpick_stats ( nick TEXT, castle TEXT, success BOOLEAN, time FLOAT, timestamp TIMESTAMPTZ DEFAULT now() ); """) conn.commit() cur.close() conn.close() print("[DEBUG] Baza danych zainicjalizowana")

def fetch_all_log_files(): print("[DEBUG] Nawiązywanie połączenia FTP...") ftp = FTP() ftp.connect(FTP_HOST, FTP_PORT) ftp.login(FTP_USER, FTP_PASS) ftp.cwd(LOG_DIR) filenames = [fn for fn in ftp.nlst() if re.match(r"gameplay_.*\.log", fn)]

logs = {}
for filename in filenames:
    print(f"[DEBUG] Pobieranie pliku: {filename}")
    with io.BytesIO() as bio:
        ftp.retrbinary(f"RETR {filename}", bio.write)
        content = bio.getvalue().decode("utf-16-le", errors="ignore")
        logs[filename] = content

ftp.quit()
print(f"[DEBUG] Pobrano {len(logs)} plików logów")
return logs

def parse_logs(logs): pattern = re.compile(r"(.?) - (.?) tried to pick the lock on (.*?) and (succeeded|failed) after (\d+) seconds") entries = [] for filename, content in logs.items(): for line in content.splitlines(): match = pattern.search(line) if match: nick = match.group(2) castle = match.group(3) success = match.group(4) == "succeeded" seconds = int(match.group(5)) entries.append((nick, castle, success, seconds)) print(f"[DEBUG] Przetworzono {len(entries)} wpisów z logów") return entries

def store_entries(entries): if not entries: print("[DEBUG] Brak nowych wpisów do zapisania") return

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
for entry in entries:
    cur.execute("""
        INSERT INTO lockpick_stats (nick, castle, success, time)
        VALUES (%s, %s, %s, %s)
    """, entry)
conn.commit()
cur.close()
conn.close()
print(f"[DEBUG] Zapisano {len(entries)} nowych wpisów do bazy danych")

def generate_table(): conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() cur.execute(""" SELECT nick, castle, COUNT() as total, COUNT() FILTER (WHERE success) as success_count, COUNT() FILTER (WHERE NOT success) as fail_count, ROUND(COUNT() FILTER (WHERE success) * 100.0 / NULLIF(COUNT(*), 0), 1) as effectiveness, ROUND(AVG(time), 1) as avg_time FROM lockpick_stats GROUP BY nick, castle ORDER BY effectiveness DESC, total DESC; """) rows = cur.fetchall() cur.close() conn.close()

if not rows:
    print("[DEBUG] Brak danych do wygenerowania tabeli")
    return None

headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]

def format_row(row):
    return "| " + " | ".join(f"{str(col):^{col_widths[i]}}" for i, col in enumerate(row)) + " |"

table = [format_row(headers), format_row(["-" * w for w in col_widths])]
for row in rows:
    table.append(format_row(row))

return "```\n" + "\n".join(table) + "\n```"

def send_to_webhook(message): if message: requests.post(WEBHOOK_URL, json={"content": message}) print("[DEBUG] Wysłano dane na webhook")

def process_logs(): logs = fetch_all_log_files() entries = parse_logs(logs) store_entries(entries) return entries

def main_loop(): init_db() new_entries = process_logs() if new_entries: message = generate_table() send_to_webhook(message) else: print("[DEBUG] Brak nowych danych - webhook nie został wysłany")

app = Flask(name)

@app.route("/") def index(): return "Alive"

if name == "main": main_loop() app.run(host='0.0.0.0', port=3000)

