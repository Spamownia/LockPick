import ftplib
import io
import re
import pandas as pd
import psycopg2
import ssl
from tabulate import tabulate
from flask import Flask
from datetime import datetime
import time

# === KONFIGURACJE ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# === FLASK ===
app = Flask(__name__)

# === FUNKCJE ===

def fetch_log_files_from_ftp():
    print("🔄 Rozpoczynanie pobierania logów z FTP...")
    logs = []
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(LOG_DIR)

        files = []
        ftp.retrlines('LIST', lambda line: files.append(line.split()[-1]))

        for filename in files:
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                print(f"📁 Pobieranie: {filename}")
                bio = io.BytesIO()
                ftp.retrbinary(f"RETR {filename}", bio.write)
                content = bio.getvalue().decode('utf-16-le', errors='ignore')
                logs.append(content)
    print(f"✅ Pobieranie zakończone. Liczba plików: {len(logs)}")
    return "\n".join(logs)


def parse_log_content(log_data):
    print("🔍 Analiza logów...")
    pattern = re.compile(
        r'\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<user>.*?) \[.*?\] Lock: (?P<lock>.*?) Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>\d+\.\d+)'
    )

    data = []
    for match in pattern.finditer(log_data):
        data.append({
            "Nick": match.group("user"),
            "Zamek": match.group("lock"),
            "Sukces": match.group("success") == "Yes",
            "Czas (s)": float(match.group("time"))
        })

    print(f"✅ Rozpoznano poprawnie wpisów: {len(data)}")
    return pd.DataFrame(data)


def aggregate_statistics(df):
    print("📊 Agregowanie danych...")
    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Ilość=('Sukces', 'count'),
        Udane=('Sukces', 'sum'),
        Nieudane=('Sukces', lambda x: (~x).sum()),
        Skuteczność=('Sukces', lambda x: f"{(x.sum() / len(x)) * 100:.2f}%"),
        Średni_czas=('Czas (s)', lambda x: f"{x.mean():.2f}s")
    ).reset_index()
    return grouped


def format_table(df):
    print("📋 Formatowanie tabeli...")
    table = tabulate(df, headers="keys", tablefmt="grid", showindex=False, stralign="center", numalign="center")
    return f"```{table}```"


def send_to_discord(message):
    print("📤 Wysyłanie danych do Discorda...")
    import requests
    payload = {"content": message}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"📬 Odpowiedź Discorda: {response.status_code}")


def run_analysis():
    log_data = fetch_log_files_from_ftp()
    df = parse_log_content(log_data)

    if df.empty:
        print("⚠️ Brak danych do analizy.")
        return

    summary_df = aggregate_statistics(df)
    table_message = format_table(summary_df)
    send_to_discord(table_message)
    print("✅ Operacja zakończona pomyślnie.\n")


@app.route("/")
def index():
    return "🔐 Lockpick Analyzer działa. Wejdź na /run aby uruchomić analizę."


@app.route("/run")
def trigger_run():
    run_analysis()
    return "✅ Analiza zakończona i wysłana na Discorda."

# === URUCHOMIENIE ===
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
