import os
import re
import time
import io
import pandas as pd
import psycopg2
from ftplib import FTP
from datetime import datetime
from tabulate import tabulate
import requests
from flask import Flask

# ---------------------- Konfiguracja ----------------------
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# ---------------------- Flask (do UptimeRobot) ----------------------
app = Flask(__name__)
@app.route("/")
def index():
    return "Alive"
# ---------------------------------------------------------

def connect_to_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)
    print("[OK] Połączono z FTP:", FTP_HOST + ":" + str(FTP_PORT))
    return ftp

def list_log_files(ftp):
    files = []

    def parse_line(line):
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            name = parts[8]
            if name.startswith("gameplay_") and name.endswith(".log"):
                files.append(name)

    ftp.retrlines("LIST", callback=parse_line)
    return files

def parse_log_content(content):
    pattern = re.compile(
        r"User: (?P<nick>.+?) \| Lock type: (?P<lock>.+?) \| Success: (?P<result>Yes|No)\. Elapsed time: (?P<elapsed>\d+\.\d+)s"
    )
    entries = []
    for match in pattern.finditer(content):
        entries.append({
            "nick": match.group("nick"),
            "lock": match.group("lock"),
            "result": match.group("result"),
            "elapsed": float(match.group("elapsed")),
        })
    return entries

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_stats (
            nick TEXT,
            lock TEXT,
            result TEXT,
            elapsed FLOAT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DEBUG] Tabela lockpicking_stats sprawdzona/utworzona.")

def insert_log_data(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for e in entries:
        cur.execute(
            "INSERT INTO lockpicking_stats (nick, lock, result, elapsed) VALUES (%s, %s, %s, %s)",
            (e["nick"], e["lock"], e["result"], e["elapsed"])
        )
    conn.commit()
    cur.close()
    conn.close()

def fetch_all_log_files():
    ftp = connect_to_ftp()
    files = list_log_files(ftp)
    logs = []
    for filename in files:
        buffer = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", buffer.write)
        content = buffer.getvalue().decode("utf-16-le", errors="ignore")
        logs.append(content)
        print(f"[INFO] Załadowano: {filename}")
    return ftp, logs

def summarize_all_data():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM lockpicking_stats", conn)
    conn.close()
    if df.empty:
        return None

    summary = (
        df.groupby(["nick", "lock"])
        .agg(
            total_attempts=("result", "count"),
            successes=("result", lambda x: (x == "Yes").sum()),
            failures=("result", lambda x: (x == "No").sum()),
            effectiveness=("result", lambda x: round((x == "Yes").sum() / len(x) * 100, 2)),
            avg_time=("elapsed", lambda x: round(x.mean(), 2))
        )
        .reset_index()
    )

    summary.rename(columns={
        "nick": "Nick", "lock": "Zamek", "total_attempts": "Ilość wszystkich prób",
        "successes": "Udane", "failures": "Nieudane",
        "effectiveness": "Skuteczność (%)", "avg_time": "Średni czas (s)"
    }, inplace=True)

    return summary

def send_summary_to_webhook(summary_df):
    if summary_df is None or summary_df.empty:
        print("[INFO] Brak danych do wysłania.")
        return

    headers = summary_df.columns.tolist()
    table_str = tabulate(summary_df.values.tolist(), headers=headers, tablefmt="grid", stralign="center", numalign="center")

    payload = {
        "content": f"📊 **Statystyki Lockpicking**\n```{table_str}```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[OK] Tabela wysłana na Discord.")
    else:
        print("[BŁĄD] Nie udało się wysłać na Discord:", response.text)

def main():
    print("[DEBUG] Start programu")
    init_db()

    ftp, log_contents = fetch_all_log_files()

    all_entries = []
    for content in log_contents:
        entries = parse_log_content(content)
        all_entries.extend(entries)

    print(f"[DEBUG] Wszystkich wpisów: {len(all_entries)}")
    insert_log_data(all_entries)

    summary_df = summarize_all_data()
    send_summary_to_webhook(summary_df)

# ---------------------- Uruchomienie ----------------------
if __name__ == "__main__":
    import threading
    flask_thread = threading.Thread(target=lambda: app.run(host="0.0.0.0", port=3000))
    flask_thread.daemon = True
    flask_thread.start()

    main()
