import os import re import psycopg2 import requests import threading from flask import Flask from datetime import datetime from ftplib import FTP_TLS from io import BytesIO

=== KONFIGURACJA ===

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = { "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", "dbname": "neondb", "user": "neondb_owner", "password": "npg_dRU1YCtxbh6v", "sslmode": "require" }

=== INICJALIZACJA FLASKA ===

app = Flask(name)

@app.route('/') def index(): return "Alive"

=== FUNKCJE POMOCNICZE ===

def parse_log(content): entries = [] lines = content.splitlines() for line in lines: if "Lockpicking event" in line: match = re.search(r"(\d{4}.\d{2}.\d{2}-\d{2}.\d{2}.\d{2}).*?Player (.+?) tried to lockpick the doors of (.+?). Success: (True|False) in ([\d.]+)s", line) if match: timestamp, nick, zamek, success, time = match.groups() entries.append({ "timestamp": timestamp, "nick": nick, "zamek": zamek, "success": success == "True", "time": float(time) }) return entries

def fetch_all_gameplay_logs(): print("[DEBUG] Connecting to FTP...") ftp = FTP_TLS() ftp.connect(FTP_HOST, FTP_PORT) ftp.login(FTP_USER, FTP_PASS) ftp.prot_p() ftp.cwd(FTP_DIR)

filenames = []
ftp.retrlines('LIST', lambda line: filenames.append(line.split()[-1]))
log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]

all_entries = []
for filename in log_files:
    print(f"[DEBUG] Fetching: {filename}")
    bio = BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    content = bio.getvalue().decode("utf-16-le", errors="ignore")
    entries = parse_log(content)
    all_entries.extend(entries)
    print(f"[DEBUG] Parsed {len(entries)} entries from {filename}")

ftp.quit()
print(f"[DEBUG] Łącznie wpisów: {len(all_entries)}")
return all_entries

def init_db(): print("[DEBUG] Inicjalizacja bazy danych...") conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() cur.execute(""" CREATE TABLE IF NOT EXISTS gameplay_logs ( id SERIAL PRIMARY KEY, timestamp TEXT, nick TEXT, zamek TEXT, success BOOLEAN, time FLOAT ); """) conn.commit() cur.close() conn.close()

def save_entries(entries): conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() new_count = 0 for entry in entries: cur.execute(""" SELECT 1 FROM gameplay_logs WHERE timestamp = %s AND nick = %s AND zamek = %s """, (entry['timestamp'], entry['nick'], entry['zamek'])) if not cur.fetchone(): cur.execute(""" INSERT INTO gameplay_logs (timestamp, nick, zamek, success, time) VALUES (%s, %s, %s, %s, %s) """, (entry['timestamp'], entry['nick'], entry['zamek'], entry['success'], entry['time'])) new_count += 1 conn.commit() cur.close() conn.close() print(f"[DEBUG] Zapisano {new_count} nowych wpisów do bazy.")

def fetch_stats(): conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() cur.execute(""" SELECT nick, zamek, COUNT() as total, SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count, SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as fail_count, ROUND(100.0 * SUM(CASE WHEN success THEN 1 ELSE 0 END)/COUNT(), 1) as skutecznosc, ROUND(AVG(time), 2) as sredni_czas FROM gameplay_logs GROUP BY nick, zamek ORDER BY skutecznosc DESC NULLS LAST, sredni_czas ASC; """) rows = cur.fetchall() cur.close() conn.close() return rows

def format_table(rows): headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"] col_widths = [len(h) for h in headers] for row in rows: for i, item in enumerate(row): col_widths[i] = max(col_widths[i], len(str(item)))

line = " | ".join(h.center(col_widths[i]) for i, h in enumerate(headers))
separator = "-+-".join("-" * w for w in col_widths)
lines = [line, separator]
for row in rows:
    lines.append(" | ".join(str(item).center(col_widths[i]) for i, item in enumerate(row)))
return "\n".join(lines)

def send_to_webhook(message): requests.post(WEBHOOK_URL, json={"content": f"\n{message}\n"})

=== GŁÓWNA PĘTLA ===

def main_loop(): print("[DEBUG] Start main_loop") init_db() entries = fetch_all_gameplay_logs() if entries: save_entries(entries) stats = fetch_stats() table = format_table(stats) send_to_webhook(table) else: print("[DEBUG] Brak nowych danych do przetworzenia.")

if name == 'main': threading.Thread(target=main_loop).start() app.run(host=
                                                                      '0.0.0.0', port=3000)
