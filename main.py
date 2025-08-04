import os
import time
import ftplib
import psycopg2
import pandas as pd
from io import BytesIO
from datetime import datetime
from tabulate import tabulate
from flask import Flask
import threading
import requests

# Konfiguracja
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

app = Flask(__name__)
processed_lines = set()
last_log_filename = None


def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp


def list_log_files(ftp):
    files = []

    def parse_line(line):
        parts = line.split(maxsplit=8)
        if len(parts) >= 9:
            filename = parts[8]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                files.append(filename)

    ftp.cwd(FTP_DIR)
    ftp.retrlines("LIST", callback=parse_line)
    return sorted(files)


def download_log_file(ftp, filename):
    log_bytes = BytesIO()
    ftp.retrbinary(f"RETR {FTP_DIR}{filename}", log_bytes.write)
    log_bytes.seek(0)
    return log_bytes.read().decode("utf-16-le", errors="ignore")


def parse_log_content(content):
    data = []
    for line in content.splitlines():
        if "[LogMinigame]" in line and "User:" in line:
            try:
                user_part = line.split("User:")[1].split()[0]
                lock_type = next((word for word in line.split() if "Lock" in word), "Unknown")
                success = "Yes" in line
                elapsed_time = float(line.split("Elapsed time:")[1].split()[0]) if "Elapsed time:" in line else None
                data.append((user_part, lock_type, success, elapsed_time))
            except Exception as e:
                print(f"❗ Błąd parsowania linii: {line} | {e}")
    return data


def save_to_db(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            lock TEXT,
            success BOOLEAN,
            time FLOAT
        )
    """)
    for nick, lock, success, elapsed in entries:
        cur.execute("INSERT INTO lockpicking (nick, lock, success, time) VALUES (%s, %s, %s, %s)",
                    (nick, lock, success, elapsed))
    conn.commit()
    cur.close()
    conn.close()


def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpicking", conn)
    conn.close()
    if df.empty:
        return "Brak danych."
    grouped = df.groupby(["nick", "lock"]).agg(
        Proby=("success", "count"),
        Udane=("success", "sum"),
        Nieudane=("success", lambda x: (~x).sum()),
        Skutecznosc=("success", lambda x: f"{(x.mean() * 100):.1f}%"),
        SredniCzas=("time", lambda x: f"{x.mean():.2f}s")
    ).reset_index()
    return tabulate(grouped, headers="keys", tablefmt="grid", stralign="center", numalign="center")


def send_to_discord(message):
    try:
        requests.post(WEBHOOK_URL, json={"content": f"```{message}```"})
    except Exception as e:
        print(f"❗ Błąd wysyłania na webhook: {e}")


def monitor_logs():
    global last_log_filename
    print("🔁 Uruchomiono pętlę monitorowania logów co 60s...")
    while True:
        try:
            ftp = connect_ftp()
            files = list_log_files(ftp)
            if not files:
                print("⚠️ Brak plików logów.")
                ftp.quit()
                time.sleep(60)
                continue

            latest_file = files[-1]
            if latest_file != last_log_filename:
                print(f"📄 Nowy plik logu wykryty: {latest_file}")
                content = download_log_file(ftp, latest_file)
                parsed = parse_log_content(content)
                print(f"📊 Rozpoznano {len(parsed)} poprawnych wpisów.")
                if parsed:
                    save_to_db(parsed)
                    tabela = create_dataframe()
                    send_to_discord(tabela)
                last_log_filename = latest_file
            else:
                print("⏳ Brak nowych plików logów.")

            ftp.quit()
        except Exception as e:
            print(f"❗ Błąd w pętli: {e}")

        time.sleep(60)


@app.route("/")
def index():
    return "Skrypt Lockpick działa."


if __name__ == "__main__":
    threading.Thread(target=monitor_logs, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
