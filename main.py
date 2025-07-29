import os
import time
import re
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from flask import Flask
from ftplib import FTP
from io import BytesIO

# Konfiguracja bazy danych Neon
DB_CONFIG = {
    'host': "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    'dbname': "neondb",
    'user': "neondb_owner",
    'password': "npg_dRU1YCtxbh6v",
    'sslmode': "require"
}

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Globalne ID ostatnio przetworzonej linii
last_log_line_id = None


def connect_db():
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpick_logs (
                id SERIAL PRIMARY KEY,
                nick TEXT,
                lock TEXT,
                success BOOLEAN,
                time FLOAT,
                UNIQUE(nick, lock, success, time)
            );
        """)
        conn.commit()


def parse_log_entries(content):
    pattern = re.compile(
        r"User:\s*(?P<nick>\w+).*?Type:\s*(?P<lock>\w+).*?Success:\s*(?P<success>Yes|No).*?Elapsed time:\s*(?P<time>\d+\.\d+)",
        re.DOTALL
    )
    matches = pattern.findall(content)
    return [
        {
            "nick": match[0],
            "lock": match[1],
            "success": match[2] == "Yes",
            "time": float(match[3])
        }
        for match in matches
    ]


def fetch_latest_log():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)

    filenames = []
    ftp.retrlines("MLSD", lambda line: filenames.append(line))
    log_files = [line.split(";")[-1].strip() for line in filenames if "gameplay_" in line]
    log_files.sort(reverse=True)
    latest_file = log_files[0]

    buffer = BytesIO()
    ftp.retrbinary(f"RETR {latest_file}", buffer.write)
    ftp.quit()

    return latest_file, buffer.getvalue().decode("utf-16-le", errors="ignore")


def insert_new_logs(entries):
    new_entries = 0
    with connect_db() as conn, conn.cursor() as cur:
        for entry in entries:
            try:
                cur.execute("""
                    INSERT INTO lockpick_logs (nick, lock, success, time)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (entry['nick'], entry['lock'], entry['success'], entry['time']))
                new_entries += cur.rowcount
        conn.commit()
    return new_entries


def create_dataframe():
    with connect_db() as conn:
        df = pd.read_sql_query("SELECT * FROM lockpick_logs;", conn)

    if df.empty:
        return None

    summary = (
        df.groupby(['nick', 'lock'])
        .agg(
            attempts=('success', 'count'),
            successes=('success', 'sum'),
            failures=('success', lambda x: (~x).sum()),
            avg_time=('time', 'mean')
        )
        .reset_index()
    )
    summary['accuracy'] = (summary['successes'] / summary['attempts'] * 100).round(2)

    summary = summary.rename(columns={
        'nick': 'Nick',
        'lock': 'Zamek',
        'attempts': 'Ilość wszystkich prób',
        'successes': 'Udane',
        'failures': 'Nieudane',
        'accuracy': 'Skuteczność (%)',
        'avg_time': 'Średni czas'
    })

    return summary


def send_to_discord(df):
    table = tabulate(df, headers='keys', tablefmt='grid', stralign="center", numalign="center")
    message = f"```\n{table}\n```"
    requests.post(WEBHOOK_URL, json={"content": message})


def main_loop():
    global last_log_line_id
    print("[DEBUG] Start programu")

    while True:
        try:
            print("[DEBUG] Sprawdzanie nowego logu...")
            filename, content = fetch_latest_log()

            if not content:
                print("[DEBUG] Log pusty lub niepobrany.")
                time.sleep(60)
                continue

            current_line_id = hash(content)
            if current_line_id == last_log_line_id:
                print("[DEBUG] Brak nowych wpisów w logu.")
                time.sleep(60)
                continue

            entries = parse_log_entries(content)
            print(f"[DEBUG] Rozpoznano {len(entries)} wpisów.")

            if not entries:
                print("[DEBUG] Brak rozpoznanych wpisów w ostatnim pliku.")
                last_log_line_id = current_line_id
                time.sleep(60)
                continue

            new_count = insert_new_logs(entries)
            print(f"[DEBUG] Dodano {new_count} nowych wpisów do bazy.")

            if new_count > 0:
                df = create_dataframe()
                if df is not None:
                    send_to_discord(df)
                    print("[DEBUG] Tabela wysłana na webhook.")
            else:
                print("[DEBUG] Brak nowych unikalnych wpisów.")

            last_log_line_id = current_line_id

        except Exception as e:
            print(f"[ERROR] Wystąpił błąd: {e}")

        time.sleep(60)


# Flask do pingowania
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

if __name__ == '__main__':
    init_db()
    import threading
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
