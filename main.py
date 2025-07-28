import os
import re
import time
import psycopg2
import pandas as pd
from ftplib import FTP
from io import BytesIO
from tabulate import tabulate
import requests
from datetime import datetime

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

last_known_lines = 0

def connect_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def get_latest_log_filename(ftp):
    ftp.cwd(LOG_DIR)
    files = []
    ftp.retrlines("LIST", lambda x: files.append(x))
    log_files = [f.split()[-1] for f in files if f.split()[-1].startswith("gameplay_") and f.endswith(".log")]
    return sorted(log_files)[-1] if log_files else None

def download_log_file(ftp, filename):
    bio = BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    bio.seek(0)
    return bio.read().decode("utf-16-le", errors="ignore")

def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame\].*?User:\s*(?P<nick>\w+).*?"
        r"LockType:\s*(?P<lock>[\w_]+).*?"
        r"Success:\s*(?P<success>\w+).*?"
        r"Elapsed time:\s*(?P<time>[0-9.]+)",
        re.DOTALL
    )
    return pattern.findall(content)

def create_dataframe(entries):
    rows = []
    for nick, lock, success, time_str in entries:
        success = success.lower() == "yes"
        elapsed = float(time_str)
        rows.append({"Nick": nick, "LockType": lock, "Success": success, "Time": elapsed})
    df = pd.DataFrame(rows)
    return df

def update_postgresql(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lock_stats (
            nick TEXT,
            locktype TEXT,
            success BOOLEAN,
            time FLOAT,
            timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO lock_stats (nick, locktype, success, time) VALUES (%s, %s, %s, %s)",
            (row["Nick"], row["LockType"], row["Success"], row["Time"])
        )
    conn.commit()
    cur.close()
    conn.close()

def generate_summary_table():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT nick, locktype, success, time FROM lock_stats", conn)
    conn.close()

    if df.empty:
        return "Brak danych do wyświetlenia."

    grouped = df.groupby(["Nick", "LockType"], observed=False)
    summary = []
    for (nick, lock), group in grouped:
        total = len(group)
        success = group["Success"].sum()
        fail = total - success
        avg_time = round(group["Time"].mean(), 2)
        accuracy = f"{(success / total * 100):.1f}%"
        summary.append([
            nick.center(12),
            lock.center(10),
            str(total).center(6),
            str(success).center(6),
            str(fail).center(7),
            accuracy.center(10),
            str(avg_time).center(10)
        ])

    headers = ["Nick", "Zamek", "Próby", "Udane", "Nieudane", "Skuteczność", "Śr. czas"]
    return "```\n" + tabulate(summary, headers=headers, tablefmt="grid") + "\n```"

def send_to_discord(message):
    requests.post(WEBHOOK_URL, json={"content": message})

def main_loop():
    global last_known_lines
    print("[DEBUG] Start programu")
    ftp = connect_ftp()
    latest_file = get_latest_log_filename(ftp)
    if not latest_file:
        print("[DEBUG] Brak logów.")
        return
    print(f"[DEBUG] Ostatni log: {latest_file}")
    last_content = ""

    while True:
        try:
            content = download_log_file(ftp, latest_file)
            lines = content.splitlines()
            if len(lines) > last_known_lines:
                new_lines = "\n".join(lines[last_known_lines:])
                entries = parse_log_content(new_lines)
                print(f"[DEBUG] Wczytano {len(entries)} nowych wpisów.")
                if entries:
                    df = create_dataframe(entries)
                    update_postgresql(df)
                    summary = generate_summary_table()
                    send_to_discord(summary)
                else:
                    print("[DEBUG] Brak nowych rozpoznanych wpisów.")
                last_known_lines = len(lines)
            else:
                print("[DEBUG] Brak nowych danych.")
            time.sleep(60)
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(60)

if __name__ == "__main__":
    main_loop()
