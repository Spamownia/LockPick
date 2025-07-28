import ftplib
import io
import re
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from flask import Flask
import threading
import time

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOGS_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- REGEXP DOPASOWUJĄCY WPISY ---
LOG_ENTRY_REGEX = re.compile(
    r"User: (?P<nick>.*?) \([0-9]+, (?P<steamid>\d+)\)\. "
    r"Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>\d+\.\d+)\. Failed attempts: \d+\. "
    r"Target object: .*?\. Lock type: (?P<lock_type>\w+)\. User owner:"
)

app = Flask(__name__)

def parse_log_content(content: str):
    entries = []
    for match in LOG_ENTRY_REGEX.finditer(content):
        nick = match.group("nick")
        success = match.group("success") == "Yes"
        time_elapsed = float(match.group("time"))
        lock_type = match.group("lock_type")
        entries.append({
            "Nick": nick,
            "Success": success,
            "Elapsed": time_elapsed,
            "LockType": lock_type
        })
    return entries

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    return ftp

def download_and_parse_logs():
    ftp = connect_ftp()
    ftp.cwd(LOGS_DIR)
    filenames = []
    ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
    log_files = [name for name in filenames if name.startswith("gameplay_") and name.endswith(".log")]
    all_entries = []
    for filename in log_files:
        with io.BytesIO() as bio:
            try:
                ftp.retrbinary(f"RETR {filename}", bio.write)
                bio.seek(0)
                content = bio.read().decode("utf-16-le", errors="ignore")
                entries = parse_log_content(content)
                all_entries.extend(entries)
                print(f"[DEBUG] Przetwarzanie: {filename} - znaleziono {len(entries)} wpisów")
            except Exception as e:
                print(f"[ERROR] Błąd przy {filename}: {e}")
    ftp.quit()
    return all_entries

def create_dataframe(entries):
    df = pd.DataFrame(entries)
    if df.empty:
        return df
    # Sortowanie lock_type według ustalonej kolejności
    lock_order = ["VeryEasy", "Basic", "Medium", "Advanced", "DialLock"]
    df["LockType"] = pd.Categorical(df["LockType"], categories=lock_order, ordered=True)
    grouped = df.groupby(["Nick", "LockType"])
    summary = grouped.agg(
        Wszystkie=("Success", "count"),
        Udane=("Success", "sum"),
        Nieudane=("Success", lambda x: x.size - x.sum()),
        Skutecznosc=("Success", lambda x: round(100 * x.sum() / x.size, 2)),
        Sredni_czas=("Elapsed", "mean")
    ).reset_index()
    summary["Sredni_czas"] = summary["Sredni_czas"].round(2)
    return summary.sort_values(by=["Nick", "LockType"])

def send_to_discord(df):
    if df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return
    table = tabulate(
        df,
        headers=["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"],
        tablefmt="github",
        colalign=("center",) * 7
    )
    print("[DEBUG] Tabela do wysłania:\n")
    print(table)
    payload = {"content": f"```\n{table}\n```"}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            print("[OK] Wysłano dane na Discord webhook.")
        else:
            print(f"[ERROR] Webhook zwrócił status: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Błąd przy wysyłaniu na webhook: {e}")

def main_loop():
    while True:
        print("[DEBUG] Start programu")
        try:
            entries = download_and_parse_logs()
            print(f"[DEBUG] Wszystkich wpisów: {len(entries)}")
            df = create_dataframe(entries)
            send_to_discord(df)
        except Exception as e:
            print(f"[ERROR] Wystąpił błąd w main_loop: {e}")
        time.sleep(60)

if __name__ == '__main__':
    main_loop()
    app.run(host='0.0.0.0', port=3000)
