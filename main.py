import os
import io
import ftplib
import psycopg2
import pandas as pd
from tabulate import tabulate
from flask import Flask
from datetime import datetime
import requests

# --- KONFIGURACJE ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- FLASK APP ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Lockpick log parser is running."

# --- FUNKCJE ---

def fetch_log_files_from_ftp():
    print("🔄 Rozpoczynanie pobierania logów z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)

    try:
        entries = list(ftp.mlsd())
        filenames = [name for name, facts in entries if name.startswith("gameplay_") and name.endswith(".log")]
    except ftplib.error_perm as e:
        print(f"❌ Błąd listowania plików: {e}")
        ftp.quit()
        return ""

    all_content = ""
    for filename in filenames:
        print(f"📄 Pobieranie pliku: {filename}")
        with io.BytesIO() as bio:
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode("utf-16-le", errors="ignore")
            all_content += content + "\n"

    ftp.quit()
    print("✅ Pobieranie zakończone.")
    return all_content

def parse_log_content(log_content):
    lines = log_content.splitlines()
    records = []

    for line in lines:
        if "[LogMinigame]" in line and "User:" in line and "Success:" in line:
            try:
                user_part = line.split("User:")[1].split()[0]
                lock_part = next((part for part in line.split() if part.startswith("LockType:")), None)
                success_part = line.split("Success:")[1].split(".")[0].strip()
                time_part = line.split("Elapsed time:")[1].split("s")[0].strip()

                nick = user_part
                lock_type = lock_part.split(":")[1] if lock_part else "Unknown"
                success = success_part == "Yes"
                elapsed = float(time_part)

                records.append({
                    "Nick": nick,
                    "Zamek": lock_type,
                    "Sukces": success,
                    "Czas": elapsed
                })
            except Exception as e:
                print(f"⚠️ Błąd parsowania linii: {line}\n{e}")

    print(f"🔍 Rozpoznano {len(records)} wpisów.")
    return records

def aggregate_statistics(records):
    df = pd.DataFrame(records)
    if df.empty:
        print("⚠️ Brak danych do przetworzenia.")
        return ""

    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Próby=("Sukces", "count"),
        Udane=("Sukces", "sum"),
        Średni_czas=("Czas", "mean")
    ).reset_index()

    grouped["Nieudane"] = grouped["Próby"] - grouped["Udane"]
    grouped["Skuteczność"] = grouped["Udane"] / grouped["Próby"] * 100
    grouped["Średni_czas"] = grouped["Średni_czas"].map(lambda x: f"{x:.2f} s")
    grouped["Skuteczność"] = grouped["Skuteczność"].map(lambda x: f"{x:.0f}%")

    final_df = grouped[["Nick", "Zamek", "Próby", "Udane", "Nieudane", "Skuteczność", "Średni_czas"]]
    table = tabulate(final_df, headers="keys", tablefmt="grid", stralign="center", numalign="center")

    print("📊 Tabela skuteczności wygenerowana.")
    return table

def send_to_discord(table_text):
    print("📤 Wysyłanie danych do Discorda...")
    payload = {
        "content": f"```\n{table_text}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("✅ Wysłano poprawnie.")
    else:
        print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")

def run():
    log_data = fetch_log_files_from_ftp()
    if not log_data:
        print("🚫 Brak danych do analizy.")
        return

    records = parse_log_content(log_data)
    if not records:
        print("🚫 Nie znaleziono poprawnych wpisów.")
        return

    table = aggregate_statistics(records)
    if table:
        send_to_discord(table)

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    run()
    app.run(host="0.0.0.0", port=10000)
