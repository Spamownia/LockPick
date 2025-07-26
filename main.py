# --- AUTOMATYCZNA INSTALACJA WYMAGANYCH BIBLIOTEK ---
import subprocess
import sys

for package in ["psycopg2-binary", "requests"]:
    try:
        __import__(package.replace("-", "_"))
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# --- IMPORTY ---
import re
import os
import psycopg2
import requests
from ftplib import FTP
from io import BytesIO
from flask import Flask

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-silent-poetry-a5o79i6l.eu-central-1.aws.neon.tech",
    "port": 5432,
    "database": "lockpick",
    "user": "default",
    "password": "Vj6whpSQRnLS"
}

LOG_PATTERN = re.compile(
    r'\[(?P<nick>.*?)\].*?started lockpicking (?P<lock>.*?) lock(?:.*?)?\.\s*Result: (?P<result>SUCCESS|FAIL).*?Time: (?P<time>\d+(?:[.,]\d+)?)s',
    re.IGNORECASE
)

# --- INICJALIZACJA BAZY DANYCH ---
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            castle TEXT,
            result TEXT,
            time FLOAT,
            hash TEXT UNIQUE
        )
    """)
    conn.commit()
    conn.close()

# --- POBIERANIE LOGÓW Z FTP ---
def download_gameplay_logs():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    filenames = []
    ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
    logs = []
    for name in filenames:
        if name.startswith("gameplay_") and name.endswith(".log"):
            bio = BytesIO()
            ftp.retrbinary(f"RETR {name}", bio.write)
            content = bio.getvalue().decode("utf-16-le", errors="ignore")
            logs.append(content)
    ftp.quit()
    return logs

# --- PRZETWARZANIE I ZAPIS DO BAZY ---
def process_logs(logs):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_entries = 0
    for content in logs:
        for match in LOG_PATTERN.finditer(content):
            nick = match.group("nick").strip()
            castle = match.group("lock").strip()
            result = match.group("result").upper()
            time_str = match.group("time").replace(",", ".").strip()
            time = float(time_str)
            entry_hash = f"{nick}|{castle}|{result}|{time}"
            try:
                cur.execute("""
                    INSERT INTO lockpick_logs (nick, castle, result, time, hash)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hash) DO NOTHING
                """, (nick, castle, result, time, entry_hash))
                if cur.rowcount > 0:
                    new_entries += 1
            except Exception as e:
                print("[ERROR] insert:", e)
    conn.commit()
    conn.close()
    return new_entries

# --- GENEROWANIE TABELI DISCORDA ---
def generate_discord_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT nick, castle, result, time FROM lockpick_logs")
    rows = cur.fetchall()
    conn.close()

    stats = {}
    for nick, castle, result, time in rows:
        key = (nick, castle)
        if key not in stats:
            stats[key] = {"total": 0, "success": 0, "fail": 0, "times": []}
        stats[key]["total"] += 1
        stats[key]["times"].append(time)
        if result == "SUCCESS":
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    data_rows = []
    for (nick, castle), val in stats.items():
        total = val["total"]
        success = val["success"]
        fail = val["fail"]
        accuracy = f"{(success / total * 100):.1f}%" if total else "0%"
        avg_time = f"{sum(val['times']) / total:.2f}s" if total else "0s"
        data_rows.append([nick, castle, str(total), str(success), str(fail), accuracy, avg_time])

    col_widths = [max(len(str(item)) for item in [header] + [row[i] for row in data_rows]) for i, header in enumerate(headers)]

    def format_row(row):
        return "| " + " | ".join(f"{cell:^{col_widths[i]}}" for i, cell in enumerate(row)) + " |"

    separator = "|-" + "-|-".join("-" * w for w in col_widths) + "-|"
    table = [format_row(headers), separator]
    for row in data_rows:
        table.append(format_row(row))

    return "```\n" + "\n".join(table) + "\n```"

# --- WYSYŁKA NA DISCORD ---
def send_to_discord():
    table = generate_discord_table()
    data = {"content": table}
    requests.post(WEBHOOK_URL, json=data)

# --- PUNKT WEJŚCIA ---
def main():
    print("[INFO] Inicjalizacja bazy...")
    init_db()
    print("[INFO] Pobieranie logów...")
    logs = download_gameplay_logs()
    print(f"[INFO] Znaleziono {len(logs)} plików logów.")
    print("[INFO] Przetwarzanie danych...")
    new = process_logs(logs)
    print(f"[INFO] Dodano nowych wpisów: {new}")
    if new > 0:
        print("[INFO] Wysyłanie statystyk na webhook...")
        send_to_discord()
    else:
        print("[INFO] Brak nowych danych.")

# --- ENDPOINT FLASK ---
app = Flask(__name__)
@app.route("/")
def index():
    return "Alive"
    
if __name__ == "__main__":
    main()
    app.run(host="0.0.0.0", port=3000)
