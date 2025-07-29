import os import re import time import ftplib import psycopg2 import pandas as pd from tabulate import tabulate from datetime import datetime from flask import Flask import threading import requests

Konfiguracja połączenia FTP

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

Konfiguracja bazy danych

DB_CONFIG = { "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", "dbname": "neondb", "user": "neondb_owner", "password": "npg_dRU1YCtxbh6v", "sslmode": "require" }

Konfiguracja webhooka

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

LAST_PROCESSED = {}  # {filename: set(linii)}

app = Flask(name)

@app.route('/') def index(): return "Alive"

def init_db(): conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() cur.execute(''' CREATE TABLE IF NOT EXISTS gameplay_logs ( id SERIAL PRIMARY KEY, nick TEXT, castle TEXT, success BOOLEAN, elapsed_time FLOAT, log_filename TEXT, raw_line TEXT UNIQUE ); ''') conn.commit() cur.close() conn.close() print("[DEBUG] Baza danych zainicjalizowana")

def parse_log_content(content): lines = content.splitlines() parsed = [] for line in lines: if '[LogMinigame]' in line and '[LockpickingMinigame_C]' in line and 'User:' in line: match = re.search(r'User: (.?) .?Type: (.?)..?Success: (Yes|No).*?Elapsed time: ([0-9.]+)', line) if match: nick = match.group(1).strip() castle = match.group(2).strip() success = match.group(3).strip() == 'Yes' elapsed = float(match.group(4)) parsed.append({ 'nick': nick, 'castle': castle, 'success': success, 'elapsed_time': elapsed, 'raw_line': line }) return parsed

def download_and_process_logs(): global LAST_PROCESSED try: ftp = ftplib.FTP() ftp.connect(FTP_HOST, FTP_PORT) ftp.login(FTP_USER, FTP_PASS) ftp.cwd(FTP_DIR)

filenames = []
    ftp.retrlines('LIST', lambda line: filenames.append(line.split()[-1]))
    log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    new_entries = []

    for filename in sorted(log_files):
        lines = []
        ftp.retrbinary(f"RETR {filename}", lambda data: lines.append(data))
        content = b''.join(lines).decode('utf-16-le', errors='ignore')

        parsed_entries = parse_log_content(content)
        for entry in parsed_entries:
            try:
                cur.execute('''
                    INSERT INTO gameplay_logs (nick, castle, success, elapsed_time, log_filename, raw_line)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (raw_line) DO NOTHING;
                ''', (entry['nick'], entry['castle'], entry['success'], entry['elapsed_time'], filename, entry['raw_line']))
                new_entries.append(entry)
            except Exception as e:
                print(f"[ERROR] Wstawianie do bazy nie powiodło się: {e}")

    conn.commit()
    cur.close()
    conn.close()
    ftp.quit()

    if new_entries:
        print(f"[DEBUG] Znaleziono {len(new_entries)} nowych wpisów. Wysyłam na Discord...")
        send_to_discord()
    else:
        print("[DEBUG] Brak nowych wpisów w logach.")

except Exception as e:
    print(f"[ERROR] Błąd FTP: {e}")

def create_dataframe(): conn = psycopg2.connect(**DB_CONFIG) df = pd.read_sql_query("SELECT * FROM gameplay_logs", conn) conn.close()

if df.empty:
    return "Brak danych."

grouped = df.groupby(['nick', 'castle'])

summary = grouped.agg(
    total=('success', 'count'),
    success_count=('success', lambda x: (x == True).sum()),
    fail_count=('success', lambda x: (x == False).sum()),
    efficiency=('success', lambda x: round((x == True).sum() / len(x) * 100, 2)),
    avg_time=('elapsed_time', 'mean')
).reset_index()

summary['avg_time'] = summary['avg_time'].round(2)

return tabulate(
    summary,
    headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"],
    tablefmt="github",
    stralign="center",
    numalign="center"
)

def send_to_discord(): table = create_dataframe() payload = { "content": f"``` {table}

}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code != 204:
        print(f"[ERROR] Webhook response: {response.status_code} {response.text}")
    else:
        print("[DEBUG] Tabela wysłana na Discord.")

def main_loop():
    while True:
        print("[DEBUG] Start main_loop")
        download_and_process_logs()
        time.sleep(60)

if __name__ == "__main__":
    print("[DEBUG] Inicjalizacja bazy danych...")
    init_db()
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)

