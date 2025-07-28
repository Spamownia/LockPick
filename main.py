import os
import time
import re
import pandas as pd
from ftplib import FTP
from io import BytesIO
from tabulate import tabulate
import psycopg2
import requests
from flask import Flask

# --- KONFIGURACJE ---
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

# --- APLIKACJA ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- FUNKCJA PARSUJĄCA ---
def parse_log_content(content):
    lines = content.splitlines()
    results = []
    pattern = re.compile(
        r"\[LogMinigame\].*?User: (?P<nick>\w+).*?Lock: (?P<lock_type>.+?)\..*?Success: (?P<success>Yes|No)\. Elapsed time: (?P<elapsed_time>\d+\.\d+)"
    )
    for line in lines:
        match = pattern.search(line)
        if match:
            results.append(match.groupdict())
    return results

# --- FUNKCJA PRZETWARZAJĄCA DANE ---
def create_dataframe(data):
    df = pd.DataFrame(data)
    if df.empty:
        return None
    df["elapsed_time"] = df["elapsed_time"].astype(float)
    df["success"] = df["success"].map({"Yes": 1, "No": 0})
    df["failure"] = 1 - df["success"]
    grouped = df.groupby(["nick", "lock_type"]).agg(
        Attempts=("success", "count"),
        Successes=("success", "sum"),
        Failures=("failure", "sum"),
        Accuracy=("success", lambda x: f"{(x.mean() * 100):.1f}%"),
        AvgTime=("elapsed_time", lambda x: f"{x.mean():.2f}s")
    ).reset_index()
    return grouped

# --- WYŚWIETLENIE I WYSYŁKA ---
def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return
    df = df.rename(columns={
        "nick": "Nick", "lock_type": "Zamek", "Attempts": "Próby",
        "Successes": "Udane", "Failures": "Nieudane", "Accuracy": "Skuteczność", "AvgTime": "Średni czas"
    })
    table = tabulate(df, headers="keys", tablefmt="grid", showindex=False, stralign="center", numalign="center")
    print("[DEBUG] Wygenerowana tabela:\n", table)
    try:
        requests.post(WEBHOOK_URL, json={"content": f"```\n{table}\n```"})
        print("[DEBUG] Wysłano dane do Discorda.")
    except Exception as e:
        print("[ERROR] Błąd wysyłki do Discorda:", e)

# --- FUNKCJA PRZETWARZAJĄCA NAJNOWSZY PLIK ---
def process_last_log(ftp, last_filename, last_known_entries):
    try:
        with BytesIO() as f:
            ftp.retrbinary(f"RETR {FTP_LOG_DIR}{last_filename}", f.write)
            content = f.getvalue().decode("utf-16le", errors="ignore")
        parsed = parse_log_content(content)
        if not parsed:
            print("[DEBUG] Brak rozpoznanych wpisów w ostatnim pliku.")
            return last_known_entries
        new_entries = [entry for entry in parsed if entry not in last_known_entries]
        if new_entries:
            print(f"[DEBUG] Nowe wpisy: {len(new_entries)}")
            df = create_dataframe(new_entries)
            send_to_discord(df)
            return parsed
        else:
            print("[DEBUG] Brak nowych wpisów.")
            return last_known_entries
    except Exception as e:
        print("[ERROR] Błąd przetwarzania logu:", e)
        return last_known_entries

# --- PĘTLA GŁÓWNA ---
def main_loop():
    print("[DEBUG] Start programu")
    last_known_entries = []
    last_filename = ""
    while True:
        try:
            with FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
                ftp.login(FTP_USER, FTP_PASS)
                ftp.cwd(FTP_LOG_DIR)
                filenames = []
                ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
                log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
                if not log_files:
                    print("[DEBUG] Brak plików gameplay_*.log na FTP.")
                    time.sleep(60)
                    continue
                latest_file = sorted(log_files)[-1]
                if latest_file != last_filename:
                    print(f"[DEBUG] Nowy plik: {latest_file}")
                    last_filename = latest_file
                    last_known_entries = []
                last_known_entries = process_last_log(ftp, latest_file, last_known_entries)
        except Exception as e:
            print("[ERROR] Błąd połączenia z FTP:", e)
        time.sleep(60)

# --- START ---
if __name__ == "__main__":
    import threading
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
