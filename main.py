import os
import re
import pandas as pd
import psycopg2
from tabulate import tabulate
import requests
from flask import Flask

# --- Flask dla UptimeRobot ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"
# ----------------------------

# --- Konfiguracja ---
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}
LOCK_ORDER = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4}
# ---------------------

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def create_table():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            lock TEXT,
            success BOOLEAN,
            time FLOAT
        )
    """)
    conn.commit()
    conn.close()

def parse_log_content(content):
    """
    Parsuje treść logu i wyciąga dane lockpickingu.
    """
    pattern = re.compile(
        r"\[LogMinigame\]\s+\[LockpickingMinigame_C\]\s+User:\s+(?P<nick>.*?)\s+\((?P<lock>.*?)\)\s+Success:\s+(?P<success>Yes|No)\.\s+Elapsed time:\s+(?P<time>\d+(\.\d+)?)"
    )
    matches = list(pattern.finditer(content))
    print(f"[DEBUG] Dopasowano {len(matches)} wpisów lockpickingu")

    data = []
    for match in matches:
        nick = match.group("nick")
        lock = match.group("lock")
        success = match.group("success") == "Yes"
        time = float(match.group("time"))
        data.append((nick, lock, success, time))

    return data

def insert_data(data):
    conn = connect_db()
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO lockpicking (nick, lock, success, time) VALUES (%s, %s, %s, %s)", data
    )
    conn.commit()
    conn.close()

def create_dataframe():
    conn = connect_db()
    df = pd.read_sql("SELECT * FROM lockpicking", conn)
    conn.close()

    if df.empty:
        return None

    df['total'] = 1
    df['success_count'] = df['success'].astype(int)
    df['fail_count'] = (~df['success']).astype(int)

    summary = df.groupby(['nick', 'lock']).agg({
        'total': 'sum',
        'success_count': 'sum',
        'fail_count': 'sum',
        'time': 'mean'
    }).reset_index()

    summary['success_rate'] = (summary['success_count'] / summary['total'] * 100).round(1)
    summary['time'] = summary['time'].round(1)

    summary = summary.rename(columns={
        'nick': 'Nick',
        'lock': 'Zamek',
        'total': 'Ilość wszystkich prób',
        'success_count': 'Udane',
        'fail_count': 'Nieudane',
        'success_rate': 'Skuteczność',
        'time': 'Średni czas'
    })

    # ✅ Sortowanie: najpierw Nick, potem kolejność zamków
    summary['Zamek_sort'] = summary['Zamek'].map(LOCK_ORDER)
    summary = summary.sort_values(by=['Nick', 'Zamek_sort'])
    summary = summary.drop(columns=['Zamek_sort'])

    print("[DEBUG] Tabela lockpickingu przed wysyłką:")
    print(tabulate(summary, headers='keys', tablefmt='grid', stralign='center', numalign='center'))

    return summary

def send_to_discord(df):
    if df is None or df.empty:
        print("[INFO] Brak danych do wysłania.")
        return

    table = tabulate(df, headers='keys', tablefmt='grid', stralign='center', numalign='center')
    payload = {
        "content": f"```\n{table}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Wysłano dane na webhook: {response.status_code}")

def main_loop():
    print("[DEBUG] Start main_loop")
    create_table()

    logs_dir = "./downloaded_logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    for filename in os.listdir(logs_dir):
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            path = os.path.join(logs_dir, filename)
            with open(path, "r", encoding="utf-16-le") as f:
                content = f.read()
                data = parse_log_content(content)
                if data:
                    insert_data(data)

    df = create_dataframe()
    send_to_discord(df)

if __name__ == "__main__":
    import threading
    threading.Thread(target=app.run, kwargs={"host": "0.0.0.0", "port": 3000}).start()
    main_loop()
