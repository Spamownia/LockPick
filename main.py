import ftplib
import io
import os
import time
import pandas as pd
import psycopg2
from datetime import datetime
from tabulate import tabulate
from flask import Flask
from threading import Thread

# === KONFIGURACJE ===
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

# === PARSOWANIE LOGÓW ===
def parse_log_content(content):
    lines = content.splitlines()
    entries = []

    for line in lines:
        if "[LogMinigame]" in line and "LockpickingMinigame_C" in line and "Success" in line:
            try:
                nick = line.split("User: ")[1].split(" ")[0].strip()
                lock = line.split("Target: ")[1].split(" ")[0].strip()
                success = "Yes" in line
                time_str = line.split("Elapsed time: ")[1].split(" ")[0].replace(",", ".").strip()
                elapsed = float(time_str)
                entries.append((nick, lock, success, elapsed))
            except Exception as e:
                print(f"⚠️  Błąd podczas parsowania linii: {line}\n{e}")
    return entries

# === PRZETWARZANIE DO DATAFRAME ===
def create_dataframe(entries):
    df = pd.DataFrame(entries, columns=["Nick", "Zamek", "Sukces", "Czas"])
    if df.empty:
        return df
    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Próby=("Sukces", "count"),
        Udane=("Sukces", "sum"),
        Średni_czas=("Czas", "mean")
    ).reset_index()
    grouped["Nieudane"] = grouped["Próby"] - grouped["Udane"]
    grouped["Skuteczność"] = (grouped["Udane"] / grouped["Próby"] * 100).round(2)
    grouped["Średni czas"] = grouped["Średni_czas"].round(2).astype(str) + " s"
    grouped["Skuteczność"] = grouped["Skuteczność"].astype(str) + " %"
    return grouped[["Nick", "Zamek", "Próby", "Udane", "Nieudane", "Skuteczność", "Średni czas"]]

# === WYSYŁKA NA DISCORD ===
def send_to_discord(df):
    if df.empty:
        print("ℹ️  Brak danych do wysłania na Discord.")
        return
    tabela = tabulate(df, headers='keys', tablefmt='github', stralign='center', numalign='center')
    payload = {"content": f"📊 **Statystyki lockpicków:**\n```\n{tabela}\n```"}
    try:
        import requests
        response = requests.post(WEBHOOK_URL, json=payload)
        print("✅ Wysłano dane na Discord." if response.ok else f"❌ Błąd Webhook: {response.status_code}")
    except Exception as e:
        print(f"❌ Błąd wysyłki do Discord: {e}")

# === POBIERANIE Z FTP ===
def fetch_log_files_from_ftp():
    print("🔄 Łączenie z FTP...")
    all_entries = []
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_DIR)
            print("📂 Pobieram listę plików...")

            # Ręczne filtrowanie nazw
            filenames = []
            ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
            log_files = [name for name in filenames if name.startswith("gameplay_") and name.endswith(".log")]

            print(f"📄 Znaleziono {len(log_files)} plików logów.")

            for filename in log_files:
                bio = io.BytesIO()
                ftp.retrbinary(f"RETR {filename}", bio.write)
                content = bio.getvalue().decode('utf-16-le', errors='ignore')
                entries = parse_log_content(content)
                all_entries.extend(entries)
                print(f"✅ Przetworzono plik: {filename} ({len(entries)} wpisów)")
    except Exception as e:
        print(f"❌ Błąd FTP: {e}")
    return all_entries

# === FLASK (nieaktywny interaktywnie, tylko jako host do Render) ===
app = Flask(__name__)

@app.route("/")
def index():
    return "🔒 Lockpick log analyzer działa."

# === GŁÓWNA PĘTLA CO 60s ===
def run_loop():
    print("🔁 Rozpoczynam automatyczny tryb przetwarzania co 60 sekund...")
    while True:
        print(f"[{datetime.now()}] 🔄 Pobieranie logów z FTP...")
        entries = fetch_log_files_from_ftp()
        df = create_dataframe(entries)
        send_to_discord(df)
        time.sleep(60)

# === URUCHOMIENIE ===
def run():
    thread = Thread(target=run_loop, daemon=True)
    thread.start()
    app.run(host="0.0.0.0", port=10000)

if __name__ == "__main__":
    run()
