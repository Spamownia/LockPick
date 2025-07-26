import os import re import psycopg2 import requests import threading from flask import Flask from datetime import datetime from ftplib import FTP_TLS from io import BytesIO, StringIO

DB_CONFIG = { "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", "dbname": "neondb", "user": "neondb_owner", "password": "npg_dRU1YCtxbh6v", "sslmode": "require" }

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/" WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(name)

def connect_db(): print("[INFO] Inicjalizacja bazy danych...") return psycopg2.connect(**DB_CONFIG)

def initialize_db(): conn = connect_db() cur = conn.cursor() cur.execute(""" CREATE TABLE IF NOT EXISTS lockpick_stats ( nick TEXT, castle TEXT, result TEXT, time FLOAT, timestamp TIMESTAMP, log_file TEXT ) """) conn.commit() cur.close() conn.close()

def parse_log(content, filename): entries = [] pattern = re.compile(r"(\d{4}.\d{2}.\d{2}-\d{2}.\d{2}.\d{2}).?Character '([^'])'.?Castle: (.?).*?(Succeeded|Failed) to pick the lock in ([\d.]+)s") for match in pattern.finditer(content): timestamp_str, nick, castle, result, time = match.groups() timestamp = datetime.strptime(timestamp_str, "%Y.%m.%d-%H.%M.%S") entries.append((nick, castle, result, float(time), timestamp, filename)) print(f"[DEBUG] Zparsowano {len(entries)} wpisów z pliku: {filename}") return entries

def fetch_log_files(): ftp = FTP_TLS() ftp.connect(FTP_HOST, FTP_PORT) ftp.auth() ftp.prot_p() ftp.login(FTP_USER, FTP_PASS) ftp.cwd(LOG_DIR)

entries = []
conn = connect_db()
cur = conn.cursor()

try:
    ftp.voidcmd('TYPE I')
    files = [line.split()[-1] for line in ftp.retrlines('LIST', lambda x: x) if line.endswith('.log') and 'gameplay_' in line]
except Exception as e:
    print(f"[ERROR] Nie można pobrać listy plików: {e}")
    return []

print(f"[INFO] Znaleziono {len(files)} plików logów na FTP.")

for filename in files:
    print(f"[INFO] Przetwarzanie: {filename}")
    if not filename.startswith("gameplay_") or not filename.endswith(".log"):
        continue

    r = BytesIO()
    try:
        ftp.retrbinary(f"RETR {filename}", r.write)
        r.seek(0)
        content = r.read().decode("utf-16-le", errors="ignore")
        new_entries = parse_log(content, filename)

        for entry in new_entries:
            cur.execute("""
                SELECT 1 FROM lockpick_stats
                WHERE nick = %s AND castle = %s AND result = %s AND time = %s AND timestamp = %s AND log_file = %s
            """, entry)
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO lockpick_stats (nick, castle, result, time, timestamp, log_file)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, entry)
        conn.commit()
    except Exception as e:
        print(f"[ERROR] Błąd przy przetwarzaniu {filename}: {e}")

cur.close()
conn.close()
ftp.quit()
return entries

def generate_stats(): conn = connect_db() cur = conn.cursor()

cur.execute("""
    SELECT nick, castle, COUNT(*),
           COUNT(*) FILTER (WHERE result = 'Succeeded') AS success,
           COUNT(*) FILTER (WHERE result = 'Failed') AS fail,
           ROUND(100.0 * COUNT(*) FILTER (WHERE result = 'Succeeded') / COUNT(*), 1) AS ratio,
           ROUND(AVG(time), 2) AS avgtime
    FROM lockpick_stats
    GROUP BY nick, castle
    ORDER BY ratio DESC, success DESC
""")
rows = cur.fetchall()
cur.close()
conn.close()

headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]

def fmt(row):
    return "| " + " | ".join(str(val).center(col_widths[i]) for i, val in enumerate(row)) + " |"

table = "\n".join([
    fmt(headers),
    "|" + "|".join("-" * (w + 2) for w in col_widths) + "|",
    *[fmt(row) for row in rows]
])
return table

def send_webhook(message): data = {"content": f"\n{message}\n"} response = requests.post(WEBHOOK_URL, json=data) print(f"[INFO] Webhook status: {response.status_code}")

def main_loop(): print("[DEBUG] Start main_loop") initialize_db() new_entries = fetch_log_files() if new_entries: table = generate_stats() send_webhook(table) else: print("[INFO] Brak nowych wpisów do przetworzenia.")

@app.route('/') def index(): return "Alive"

if name == 'main': threading.Thread(target=main_loop).start() app.run(host='0.0.0.0', port=3000)

