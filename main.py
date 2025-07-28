import os
import time
import re
import io
import ftplib
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from datetime import datetime
from flask import Flask

# === USTAWIENIA STAŁE ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

LOCK_ORDER = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4}

# === FLASK ===
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# === PARSOWANIE ===
def parse_log_content(content):
    results = []
    pattern = re.compile(
        r"User: (?P<nick>.+?) .*?Lock: (?P<lock>.+?) .*?Success: (?P<success>Yes|No).*?Elapsed time: (?P<time>\d+\.\d+)",
        re.DOTALL
    )
    for match in pattern.finditer(content):
        results.append({
            "Nick": match.group("nick").strip(),
            "Zamek": match.group("lock").strip(),
            "Sukces": match.group("success") == "Yes",
            "Czas": float(match.group("time"))
        })
    return results

# === PRZETWARZANIE ===
def create_dataframe(entries):
    if not entries:
        return pd.DataFrame()

    df = pd.DataFrame(entries)
    grouped = df.groupby(["Nick", "Zamek"])
    result = grouped.agg(
        Próby=('Sukces', 'count'),
        Udane=('Sukces', 'sum'),
        Nieudane=('Sukces', lambda x: (~x).sum()),
        Skuteczność=('Sukces', lambda x: round(100 * x.sum() / len(x), 1)),
        Średni_czas=('Czas', lambda x: round(x.mean(), 2))
    ).reset_index()

    result['Zamek'] = pd.Categorical(result['Zamek'], categories=LOCK_ORDER.keys(), ordered=True)
    result = result.sort_values(by=['Nick', 'Zamek'])
    return result

# === DANE Z BAZY ===
def save_to_db(entries):
    if not entries:
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_history (
            nick TEXT,
            zamek TEXT,
            sukces BOOLEAN,
            czas FLOAT,
            PRIMARY KEY (nick, zamek, sukces, czas)
        )
    """)
    for row in entries:
        try:
            cur.execute("""
                INSERT INTO lockpick_history (nick, zamek, sukces, czas)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (row['Nick'], row['Zamek'], row['Sukces'], row['Czas']))
        except Exception as e:
            print(f"[ERROR] Insert failed: {e}")
    conn.commit()
    cur.close()
    conn.close()

def load_all_entries():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT nick, zamek, sukces, czas FROM lockpick_history")
    rows = cur.fetchall()
    conn.close()
    return [{"Nick": r[0], "Zamek": r[1], "Sukces": r[2], "Czas": r[3]} for r in rows]

# === WYSYŁKA ===
def send_to_discord(df):
    if df.empty:
        print("[INFO] Brak danych do wysłania.")
        return
    print("[DEBUG] Tabela do wysłania:\n")
    print(tabulate(df, headers="keys", tablefmt="grid", showindex=False))

    df.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = tabulate(df, headers="keys", tablefmt="grid", showindex=False, colalign=("center",)*7)
    requests.post(WEBHOOK_URL, json={"content": f"```\n{table}\n```"})

# === LOGIKA FTP ===
def get_latest_log_filename(ftp):
    ftp.cwd(FTP_LOG_DIR)
    files = []
    ftp.retrlines("LIST", files.append)
    log_files = [f.split()[-1] for f in files if f.split()[-1].startswith("gameplay_") and f.split()[-1].endswith(".log")]
    if not log_files:
        return None
    return sorted(log_files)[-1]

def fetch_log_file(ftp, filename):
    ftp.cwd(FTP_LOG_DIR)
    stream = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", stream.write)
    return stream.getvalue().decode('utf-16-le')

# === PĘTLA GŁÓWNA ===
def main_loop():
    print("[DEBUG] Start main_loop")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)

    last_seen_line = ""
    last_log_file = None

    while True:
        try:
            current_log = get_latest_log_filename(ftp)
            if current_log != last_log_file:
                print(f"[INFO] Nowy log: {current_log}")
                last_log_file = current_log
                last_seen_line = ""

            content = fetch_log_file(ftp, current_log)
            lines = content.strip().splitlines()
            new_lines = []
            seen = False

            for line in lines:
                if line == last_seen_line:
                    seen = True
                    continue
                if seen or last_seen_line == "":
                    new_lines.append(line)

            if new_lines:
                print(f"[INFO] Nowych wpisów: {len(new_lines)}")
                entries = parse_log_content('\n'.join(new_lines))
                save_to_db(entries)
                all_entries = load_all_entries()
                df = create_dataframe(all_entries)
                send_to_discord(df)
                last_seen_line = lines[-1] if lines else last_seen_line
            else:
                print("[INFO] Brak nowych wpisów.")

        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(60)

# === START ===
if __name__ == '__main__':
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host='0.0.0.0', port=3000)
