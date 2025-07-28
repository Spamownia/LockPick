import os
import io
import re
import time
import pandas as pd
import psycopg2
import requests
from ftplib import FTP
from tabulate import tabulate
from flask import Flask
from datetime import datetime
from collections import defaultdict

# --- Flask keep-alive ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"

# --- Konfiguracja FTP i DB ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Funkcja pobierająca logi z FTP ---
def fetch_logs_from_ftp():
    try:
        with FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOG_DIR)
            filenames = []
            ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
            log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
            logs = []

            for filename in log_files:
                r = io.BytesIO()
                ftp.retrbinary(f"RETR {filename}", r.write)
                content = r.getvalue().decode('utf-16-le')
                logs.append((filename, content))
                print(f"[DEBUG] Pobrano {filename}, długość: {len(content)} znaków")
            return logs
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania logów: {e}")
        return []

# --- Parsowanie logów ---
def parse_log_content(log_content):
    entries = []
    pattern = re.compile(
        r"\[LogMinigame\].+?User: (?P<nick>.+?) .+?Type: (?P<lock>.+?) .+?Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[0-9.]+)",
        re.DOTALL
    )
    for match in pattern.finditer(log_content):
        entries.append({
            "Nick": match.group("nick"),
            "Zamek": match.group("lock"),
            "Sukces": match.group("success") == "Yes",
            "Czas": float(match.group("time"))
        })
    return entries

# --- Tworzenie tabeli z wynikami ---
def create_dataframe(parsed_entries):
    if not parsed_entries:
        return None

    df = pd.DataFrame(parsed_entries)
    grouped = df.groupby(["Nick", "Zamek"])

    result = []
    for (nick, zamek), group in grouped:
        total = len(group)
        success_count = group["Sukces"].sum()
        fail_count = total - success_count
        avg_time = group["Czas"].mean() if success_count > 0 else 0
        accuracy = (success_count / total) * 100 if total else 0

        result.append({
            "Nick": nick,
            "Zamek": zamek,
            "Ilość wszystkich prób": total,
            "Udane": success_count,
            "Nieudane": fail_count,
            "Skuteczność": f"{accuracy:.1f}%",
            "Średni czas": f"{avg_time:.2f}s"
        })

    lock_order = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4}
    result.sort(key=lambda x: (x["Nick"], lock_order.get(x["Zamek"], 99)))
    return pd.DataFrame(result)

# --- Wysyłka na Discord ---
def send_to_discord(df):
    if df is None or df.empty:
        print("[INFO] Brak danych do wysłania.")
        return

    table_str = tabulate(df, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table_str}\n```"})
    if response.status_code == 204:
        print("[INFO] Wysłano tabelę na Discord.")
    else:
        print(f"[ERROR] Nie udało się wysłać danych: {response.status_code} {response.text}")

# --- Główna pętla ---
def main_loop():
    print("[DEBUG] Start main_loop")
    logs = fetch_logs_from_ftp()
    all_entries = []

    for filename, content in logs:
        parsed = parse_log_content(content)
        print(f"[DEBUG] {filename}: {len(parsed)} rozpoznanych wpisów.")
        all_entries.extend(parsed)

    df = create_dataframe(all_entries)
    send_to_discord(df)

# --- Uruchomienie ---
if __name__ == "__main__":
    main_loop()
    app.run(host='0.0.0.0', port=3000)
