import os
import ssl
import pandas as pd
import psycopg2
import re
import requests
from io import StringIO
from datetime import datetime
from tabulate import tabulate
from flask import Flask
from ftplib import FTP_TLS

# --- Flask keepalive ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"
# -----------------------

# --- Konfiguracja ---
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

LOCK_ORDER = {'VeryEasy': 0, 'Basic': 1, 'Medium': 2, 'Advanced': 3, 'DialLock': 4}

# --- Parsowanie wpisów z logu ---
def parse_log_content(content):
    print("[DEBUG] Parsowanie zawartości logu...")
    decoded = content.decode('utf-16-le')
    pattern = r'\[LogMinigame\] \[LockpickingMinigame_C\] User: (.+?) \| Lock: (.+?) \| Success: (Yes|No)\. Elapsed time: ([\d.]+)'
    matches = re.findall(pattern, decoded)
    print(f"[DEBUG] Dopasowano wpisów: {len(matches)}")
    return [
        {
            "user": match[0].strip(),
            "lock": match[1].strip(),
            "success": match[2] == "Yes",
            "time": float(match[3])
        } for match in matches
    ]

# --- Pobranie listy plików z FTP ---
def list_log_files():
    print("[DEBUG] Nawiązywanie połączenia FTP...")
    ftps = FTP_TLS()
    ftps.ssl_version = ssl.PROTOCOL_TLSv1_2
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.auth()
    ftps.login(FTP_USER, FTP_PASS)
    ftps.prot_p()
    ftps.cwd(FTP_DIR)
    files = []
    ftps.retrlines("LIST", lambda x: files.append(x.split()[-1]) if "gameplay_" in x else None)
    ftps.quit()
    print(f"[DEBUG] Znaleziono plików: {len(files)}")
    return files

# --- Pobieranie i parsowanie plików ---
def download_and_parse_logs():
    files = list_log_files()
    all_entries = []

    ftps = FTP_TLS()
    ftps.ssl_version = ssl.PROTOCOL_TLSv1_2
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.auth()
    ftps.login(FTP_USER, FTP_PASS)
    ftps.prot_p()
    ftps.cwd(FTP_DIR)

    for file in files:
        print(f"[DEBUG] Przetwarzanie pliku: {file}")
        buffer = []
        ftps.retrlines(f"RETR {file}", buffer.append)
        content = "\n".join(buffer).encode("utf-16-le")
        entries = parse_log_content(content)
        all_entries.extend(entries)

    ftps.quit()
    return all_entries

# --- Tworzenie tabeli ---
def create_dataframe(entries):
    print("[DEBUG] Tworzenie tabeli...")
    df = pd.DataFrame(entries)
    if df.empty:
        print("[DEBUG] Brak danych do przetworzenia.")
        return pd.DataFrame()

    grouped = df.groupby(['user', 'lock']).agg(
        total=('success', 'count'),
        success=('success', 'sum'),
        fail=('success', lambda x: (~x).sum()),
        effectiveness=('success', 'mean'),
        avg_time=('time', 'mean')
    ).reset_index()

    grouped['effectiveness'] = (grouped['effectiveness'] * 100).round(1)
    grouped['avg_time'] = grouped['avg_time'].round(2)

    grouped = grouped.sort_values(by=['user', 'lock'], key=lambda col: col.map(LOCK_ORDER) if col.name == 'lock' else col)
    return grouped

# --- Wysyłka na Discord ---
def send_to_discord(df):
    if df.empty:
        print("[DEBUG] Brak danych do wysyłki.")
        return
    table = tabulate(
        df,
        headers=["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność (%)", "Średni czas"],
        tablefmt="github",
        showindex=False,
        numalign="center",
        stralign="center"
    )
    print("[DEBUG] Tabela do wysyłki:")
    print(table)

    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table}\n```"})
    print(f"[DEBUG] Wysłano na webhook, status: {response.status_code}")

# --- Główna pętla ---
def main_loop():
    print("[DEBUG] Start programu")
    entries = download_and_parse_logs()
    df = create_dataframe(entries)
    send_to_discord(df)

# --- Start programu ---
if __name__ == "__main__":
    main_loop()
    app.run(host='0.0.0.0', port=3000)
