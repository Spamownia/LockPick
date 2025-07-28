import ftplib
import os
import io
import re
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate

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

# --- FUNKCJE ---

def parse_log_content(content: str):
    entries = []

    for match in LOG_ENTRY_REGEX.finditer(content):
        nick = match.group("nick")
        success = match.group("success") == "Yes"
        time = float(match.group("time"))
        lock_type = match.group("lock_type")
        entries.append({
            "Nick": nick,
            "Success": success,
            "Elapsed": time,
            "LockType": lock_type
        })

    return entries

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    return ftp

def download_log_files():
    ftp = connect_ftp()
    ftp.cwd(LOGS_DIR)
    filenames = []
    ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))

    log_files = [name for name in filenames if name.startswith("gameplay_") and name.endswith(".log")]

    logs_data = []
    for filename in log_files:
        with io.BytesIO() as bio:
            try:
                ftp.retrbinary(f"RETR {filename}", bio.write)
                bio.seek(0)
                content = bio.read().decode("utf-16-le", errors="ignore")
                logs_data.append((filename, content))
                print(f"[INFO] Pobrano: {filename}")
            except Exception as e:
                print(f"[BŁĄD] Nie udało się pobrać {filename}: {e}")
    ftp.quit()
    return logs_data

def create_dataframe(entries):
    df = pd.DataFrame(entries)
    if df.empty:
        return df

    grouped = df.groupby(["Nick", "LockType"])
    summary = grouped.agg(
        Total=("Success", "count"),
        Successes=("Success", "sum"),
        Fails=("Success", lambda x: x.size - x.sum()),
        Accuracy=("Success", lambda x: round(100 * x.sum() / x.size, 2)),
        AvgTime=("Elapsed", "mean")
    ).reset_index()

    summary["AvgTime"] = summary["AvgTime"].round(2)
    return summary

def send_to_discord(df):
    if df.empty:
        print("[INFO] Brak danych do wysłania.")
        return

    table = tabulate(
        df,
        headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"],
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
            print(f"[BŁĄD] Webhook zwrócił status: {response.status_code}")
    except Exception as e:
        print(f"[BŁĄD] Błąd przy wysyłaniu na webhook: {e}")

def main():
    print("[DEBUG] Start analizy logów...")
    logs = download_log_files()
    all_entries = []

    for filename, content in logs:
        entries = parse_log_content(content)
        all_entries.extend(entries)
        print(f"[DEBUG] Przetworzono {len(entries)} wpisów z pliku: {filename}")

    df = create_dataframe(all_entries)
    send_to_discord(df)

if __name__ == "__main__":
    main()
