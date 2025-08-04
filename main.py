import os
import re
import time
import ssl
import psycopg2
import pandas as pd
from io import StringIO
from flask import Flask
from ftplib import FTP
from tabulate import tabulate
import threading
import datetime
import requests

# Konfiguracja
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    'host': "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    'dbname': "neondb",
    'user': "neondb_owner",
    'password': "npg_dRU1YCtxbh6v",
    'sslmode': "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
LAST_PROCESSED = {}

# Parsowanie zawartości logu
def parse_log_content(content):
    lines = content.splitlines()
    results = []
    for line in lines:
        if "[LogMinigame]" in line and "User:" in line:
            try:
                user = re.search(r"User: (\w+)", line).group(1)
                lock = re.search(r"Lock: (\w+)", line).group(1)
                success = re.search(r"Success: (Yes|No)", line).group(1)
                elapsed = re.search(r"Elapsed time: ([\d\.]+)", line)
                elapsed_time = float(elapsed.group(1)) if elapsed else None
                results.append({
                    "Nick": user,
                    "Zamek": lock,
                    "Sukces": success,
                    "Czas": elapsed_time
                })
            except Exception as e:
                print(f"Błąd parsowania linii: {line}\n{e}")
    return results

# Łączenie z FTP i pobieranie logów
def ftp_get_logs():
    print("🔗 Łączenie z FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    listing = []
    ftp.retrlines("LIST", listing.append)
    files = [line.split()[-1] for line in listing if line.endswith(".log") and line.startswith("-") and "gameplay_" in line]
    logs = {}

    for filename in files:
        buffer = []
        ftp.retrlines(f"RETR {filename}", buffer.append)
        content = "\n".join(buffer)
        decoded = content.encode("utf-8").decode("utf-16-le", errors="ignore")
        logs[filename] = decoded
        print(f"📁 Załadowano plik: {filename} ({len(decoded)} znaków)")

    ftp.quit()
    return logs

# Zapis danych do PostgreSQL
def save_to_db(data):
    if not data:
        return
    print("💾 Zapis do bazy danych...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logi (
            nick TEXT,
            zamek TEXT,
            sukces TEXT,
            czas FLOAT
        );
    """)
    for row in data:
        cur.execute("INSERT INTO logi (nick, zamek, sukces, czas) VALUES (%s, %s, %s, %s);",
                    (row["Nick"], row["Zamek"], row["Sukces"], row["Czas"]))
    conn.commit()
    cur.close()
    conn.close()

# Tworzenie i wysyłanie tabeli
def send_to_discord():
    print("📊 Generowanie tabeli i wysyłka...")
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM logi;", conn)
    conn.close()
    if df.empty:
        print("⚠️ Brak danych do wysłania.")
        return

    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Proby=("Sukces", "count"),
        Udane=("Sukces", lambda x: (x == "Yes").sum()),
        Nieudane=("Sukces", lambda x: (x == "No").sum()),
        Skutecznosc=("Sukces", lambda x: round((x == "Yes").sum() / len(x) * 100, 2)),
        SredniCzas=("Czas", lambda x: round(x.mean(), 2))
    ).reset_index()

    table = tabulate(grouped, headers=["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="github", stralign="center", numalign="center")
    payload = {"content": f"```\n{table}\n```"}
    requests.post(WEBHOOK_URL, json=payload)

# Monitorowanie nowego logu
def monitor_logs():
    print("🔁 Uruchomiono pętlę monitorowania logów co 60s...")
    while True:
        logs = ftp_get_logs()
        if not logs:
            print("⚠️ Brak logów na FTP.")
            time.sleep(60)
            continue

        latest_file = sorted(logs.keys())[-1]
        content = logs[latest_file]

        if LAST_PROCESSED.get(latest_file) == content:
            print(f"⏳ Brak zmian w pliku {latest_file}")
        else:
            print(f"🔍 Wykryto zmiany w {latest_file}")
            parsed = parse_log_content(content)
            save_to_db(parsed)
            send_to_discord()
            LAST_PROCESSED[latest_file] = content

        time.sleep(60)

# Inicjalizacja — przetwarzanie wszystkich logów
def initial_process():
    print("🚀 Inicjalne przetwarzanie wszystkich logów...")
    logs = ftp_get_logs()
    all_data = []
    for name, content in logs.items():
        parsed = parse_log_content(content)
        all_data.extend(parsed)
        LAST_PROCESSED[name] = content
    save_to_db(all_data)
    send_to_discord()

# Flask
app = Flask(__name__)

@app.route("/")
def index():
    return "Lockpick log processor is running."

# Start
if __name__ == "__main__":
    initial_process()
    threading.Thread(target=monitor_logs, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
