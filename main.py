import os
import ftplib
import pandas as pd
import psycopg2
import requests
from io import BytesIO
from datetime import datetime
from tabulate import tabulate
from flask import Flask

# --- Konfiguracje ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOGS_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

LOCK_ORDER = {'VeryEasy': 0, 'Basic': 1, 'Medium': 2, 'Advanced': 3, 'DialLock': 4}

# --- Flask ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- Funkcje ---

def connect_to_ftp():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        print("[DEBUG] Połączono z FTP")
        return ftp
    except Exception as e:
        print(f"[ERROR] Połączenie FTP nieudane: {e}")
        return None

def list_log_files(ftp):
    try:
        ftp.cwd(FTP_LOGS_DIR)
        files = []
        ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
        log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
        print(f"[DEBUG] Znaleziono plików: {len(log_files)}")
        return log_files
    except Exception as e:
        print(f"[ERROR] Nie można pobrać listy plików: {e}")
        return []

def download_log_file(ftp, filename):
    try:
        bio = BytesIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        content = bio.getvalue().decode("utf-16-le", errors="ignore")
        print(f"[DEBUG] Pobrano plik: {filename}")
        return content
    except Exception as e:
        print(f"[ERROR] Błąd pobierania {filename}: {e}")
        return ""

def parse_log_content(content):
    import re
    pattern = re.compile(
        r"User: (?P<nick>.*?) .*?Lock: (?P<lock>.*?)\..*?Success: (?P<success>Yes|No).*?Elapsed time: (?P<time>\d+\.\d+)",
        re.DOTALL
    )
    results = []
    for match in pattern.finditer(content):
        nick = match.group("nick").strip()
        lock = match.group("lock").strip()
        success = match.group("success") == "Yes"
        time = float(match.group("time"))
        results.append((nick, lock, success, time))
    return results

def create_dataframe(all_entries):
    df = pd.DataFrame(all_entries, columns=["Nick", "Lock", "Success", "Time"])
    grouped = df.groupby(["Nick", "Lock"])
    summary = grouped.agg(
        Attempts=("Success", "count"),
        Successes=("Success", "sum"),
        Failures=("Success", lambda x: (~x).sum()),
        Accuracy=("Success", "mean"),
        AvgTime=("Time", "mean")
    ).reset_index()

    summary["Accuracy"] = (summary["Accuracy"] * 100).round(1).astype(str) + "%"
    summary["AvgTime"] = summary["AvgTime"].round(2)

    # Dodane sortowanie: najpierw po Nicku, potem Lock według porządku LOCK_ORDER
    summary["LockOrder"] = summary["Lock"].map(LOCK_ORDER).fillna(999)
    summary.sort_values(by=["Nick", "LockOrder"], inplace=True)
    summary.drop(columns=["LockOrder"], inplace=True)

    print("[DEBUG] Tabela podsumowania:")
    print(tabulate(summary, headers="keys", tablefmt="grid", showindex=False))

    return summary

def send_to_discord(df):
    table = tabulate(df, headers="keys", tablefmt="grid", showindex=False, stralign="center")
    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[DEBUG] Wysłano dane do Discord webhook")
    else:
        print(f"[ERROR] Nie udało się wysłać na Discord: {response.status_code}")

def main():
    print("[DEBUG] Start programu")
    ftp = connect_to_ftp()
    if not ftp:
        return

    log_files = list_log_files(ftp)
    all_entries = []

    for filename in log_files:
        content = download_log_file(ftp, filename)
        entries = parse_log_content(content)
        print(f"[DEBUG] W pliku {filename} znaleziono wpisów: {len(entries)}")
        all_entries.extend(entries)

    ftp.quit()

    if not all_entries:
        print("[INFO] Brak nowych danych do przetworzenia.")
        return

    df = create_dataframe(all_entries)
    send_to_discord(df)

if __name__ == "__main__":
    main()
    app.run(host="0.0.0.0", port=3000)
