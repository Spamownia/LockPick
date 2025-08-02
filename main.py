import os
import ftplib
import io
import time
import pandas as pd
import psycopg2
from datetime import datetime
from flask import Flask
from tabulate import tabulate

# === KONFIGURACJA ===
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

# === FLASK ===
app = Flask(__name__)

@app.route("/")
def index():
    return "Lockpick Analyzer is running."

# === FUNKCJA POBIERANIA LOGÓW Z FTP ===
def fetch_log_files_from_ftp():
    log_data = {}
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)
        entries = []
        ftp.retrlines('LIST', entries.append)
        filenames = [entry.split()[-1] for entry in entries if entry.endswith('.log') and entry.startswith('gameplay_')]
        print(f"📄 Znalezione pliki: {filenames}")
        for filename in filenames:
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            bio.seek(0)
            content = bio.read().decode('utf-16-le')
            log_data[filename] = content
    return log_data

# === PARSOWANIE TREŚCI LOGÓW ===
def parse_log_content(log_data):
    parsed_entries = []
    for filename, content in log_data.items():
        for line in content.splitlines():
            if "[LogMinigame]" in line and "LockpickingMinigame_C" in line:
                try:
                    nick = line.split("User: ")[1].split()[0]
                    lock_type = line.split("Lock difficulty: ")[1].split()[0]
                    success = "Yes" in line.split("Success: ")[1]
                    elapsed = float(line.split("Elapsed time: ")[1].split()[0])
                    parsed_entries.append({
                        "Nick": nick,
                        "Zamek": lock_type,
                        "Sukces": success,
                        "Czas": elapsed
                    })
                except Exception as e:
                    print(f"⚠️ Błąd parsowania linii: {line}\n{e}")
    print(f"📊 Przetworzono wpisów: {len(parsed_entries)}")
    return pd.DataFrame(parsed_entries)

# === TWORZENIE TABELI ===
def create_dataframe(df):
    if df.empty:
        print("⚠️ Brak danych do analizy.")
        return None
    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Próby=('Sukces', 'count'),
        Udane=('Sukces', 'sum'),
        Średni_czas=('Czas', 'mean')
    ).reset_index()
    grouped["Nieudane"] = grouped["Próby"] - grouped["Udane"]
    grouped["Skuteczność"] = (grouped["Udane"] / grouped["Próby"] * 100).round(2).astype(str) + " %"
    grouped["Średni czas"] = grouped["Średni_czas"].round(2).astype(str) + " s"
    grouped = grouped.drop(columns=["Średni_czas"])
    print(f"📈 Gotowa tabela:\n{grouped}")
    return grouped

# === WYSYŁANIE NA DISCORD ===
def send_to_discord(df):
    if df is None or df.empty:
        print("📭 Brak danych do wysłania.")
        return
    table_str = tabulate(df, headers="keys", tablefmt="grid", stralign="center")
    import requests
    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table_str}\n```"})
    print(f"📤 Wysłano do Discord (status: {response.status_code})")

# === GŁÓWNA FUNKCJA ===
def run():
    print("🔁 Rozpoczynam automatyczny tryb przetwarzania co 60 sekund...")
    while True:
        print(f"[{datetime.now()}] 🔄 Pobieranie logów z FTP...")
        try:
            log_data = fetch_log_files_from_ftp()
            df = parse_log_content(log_data)
            grouped_df = create_dataframe(df)
            send_to_discord(grouped_df)
        except Exception as e:
            print(f"❌ Błąd podczas przetwarzania: {e}")
        time.sleep(60)

# === START APLIKACJI ===
if __name__ == "__main__":
    import threading
    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
    app.run(host="0.0.0.0", port=10000)
