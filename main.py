import os
import re
import io
import time
import ftplib
import psycopg2
import pandas as pd
from tabulate import tabulate
from datetime import datetime
from flask import Flask, jsonify
import requests

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# Konfiguracja bazy danych
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"status": "OK"})


def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def init_db():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicking_stats (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    lock_type TEXT,
                    success BOOLEAN,
                    time FLOAT,
                    timestamp TIMESTAMP
                )
            """)
        conn.commit()
    print("[DEBUG] Inicjalizacja bazy danych zakończona")


def list_gameplay_logs(ftp):
    try:
        ftp.cwd(FTP_LOG_PATH)
        files = []
        ftp.retrlines('LIST', lambda x: files.append(x))
        gameplay_logs = [
            re.split(r'\s+', line)[-1]
            for line in files
            if re.search(r'gameplay_.*\.log$', line)
        ]
        return gameplay_logs
    except Exception as e:
        print(f"[ERROR] Błąd podczas listowania plików: {e}")
        return []


def download_logs_from_ftp():
    logs = []
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            log_files = list_gameplay_logs(ftp)

            for file in log_files:
                try:
                    bio = io.BytesIO()
                    ftp.retrbinary(f'RETR {file}', bio.write)
                    content = bio.getvalue().decode('utf-16-le', errors='ignore')
                    logs.append(content)
                    print(f"[DEBUG] Pobrano plik: {file}")
                except Exception as e:
                    print(f"[ERROR] Nie udało się pobrać pliku {file}: {e}")
    except Exception as e:
        print(f"[ERROR] Błąd połączenia z FTP: {e}")
    return logs


def parse_log_content(log_text):
    pattern = r'\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>\w+).*?Type: (?P<lock_type>\w+).*?Success: (?P<success>\w+).*?Elapsed time: (?P<time>\d+\.\d+)'
    matches = re.finditer(pattern, log_text)

    parsed = []
    for match in matches:
        nick = match.group("nick")
        lock_type = match.group("lock_type")
        success = match.group("success") == "Yes"
        elapsed_time = float(match.group("time"))
        parsed.append((nick, lock_type, success, elapsed_time, datetime.utcnow()))
    print(f"[DEBUG] Rozpoznano {len(parsed)} wpisów w logu")
    return parsed


def insert_new_entries(parsed_data):
    if not parsed_data:
        print("[DEBUG] Brak danych do zapisania")
        return False

    new_entries = 0
    with connect_db() as conn:
        with conn.cursor() as cur:
            for nick, lock_type, success, time_val, timestamp in parsed_data:
                cur.execute("""
                    SELECT 1 FROM lockpicking_stats
                    WHERE nick = %s AND lock_type = %s AND success = %s AND time = %s
                """, (nick, lock_type, success, time_val))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO lockpicking_stats (nick, lock_type, success, time, timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (nick, lock_type, success, time_val, timestamp))
                    new_entries += 1
        conn.commit()
    print(f"[DEBUG] Dodano {new_entries} nowych wpisów do bazy")
    return new_entries > 0


def create_dataframe():
    with connect_db() as conn:
        df = pd.read_sql("SELECT * FROM lockpicking_stats", conn)

    if df.empty:
        print("[DEBUG] Brak danych do wyświetlenia")
        return None

    grouped = df.groupby(['nick', 'lock_type']).agg(
        total_tries=('success', 'count'),
        successes=('success', 'sum'),
        fails=('success', lambda x: (~x).sum()),
        accuracy=('success', 'mean'),
        avg_time=('time', 'mean')
    ).reset_index()

    grouped['accuracy'] = (grouped['accuracy'] * 100).round(1).astype(str) + '%'
    grouped['avg_time'] = grouped['avg_time'].round(2)

    grouped = grouped.rename(columns={
        'nick': 'Nick',
        'lock_type': 'Zamek',
        'total_tries': 'Ilość wszystkich prób',
        'successes': 'Udane',
        'fails': 'Nieudane',
        'accuracy': 'Skuteczność',
        'avg_time': 'Średni czas'
    })

    print("[DEBUG] Przygotowano ramkę danych do wysyłki")
    return grouped


def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania na Discorda")
        return

    headers = df.columns.tolist()
    rows = df.values.tolist()
    table = tabulate(rows, headers, tablefmt="github", stralign="center", numalign="center")

    data = {
        "content": f"```\n{table}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code == 204:
        print("[DEBUG] Tabela wysłana na Discorda")
    else:
        print(f"[ERROR] Nie udało się wysłać na Discorda: {response.status_code}")


def main():
    print("[DEBUG] Start procesu")
    init_db()
    logs = download_logs_from_ftp()
    all_parsed = []
    for log in logs:
        all_parsed.extend(parse_log_content(log))

    if insert_new_entries(all_parsed):
        df = create_dataframe()
        send_to_discord(df)
    else:
        print("[DEBUG] Brak nowych danych – wysyłka pominięta")


if __name__ == "__main__":
    main()
    app.run(host="0.0.0.0", port=3000)
