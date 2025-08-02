from flask import Flask
import ftplib
import io
import pandas as pd
import re
import ssl
from datetime import datetime
from tabulate import tabulate
import requests
import threading
import time

# ---------- Konfiguracja ----------

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)

# ---------- Funkcje ----------

def fetch_log_files_from_ftp():
    print(f"[{datetime.now()}] 🔄 Pobieranie logów z FTP...")
    log_contents = []
    try:
        context = ssl._create_unverified_context()
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOG_DIR)
            filenames = ftp.nlst()

            for filename in filenames:
                if filename.startswith("gameplay_") and filename.endswith(".log"):
                    print(f"📁 Pobieranie pliku: {filename}")
                    content = io.BytesIO()
                    ftp.retrbinary(f"RETR {filename}", content.write)
                    content.seek(0)
                    log_contents.append(content.read().decode("utf-16-le"))
    except Exception as e:
        print(f"❌ Błąd FTP: {e}")

    print(f"✅ Liczba plików pobranych: {len(log_contents)}\n")
    return log_contents


def parse_log_content(log_contents):
    print(f"[{datetime.now()}] 🧩 Parsowanie zawartości logów...")
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (.*?) "
        r"started lockpicking. Lock: (.*?)\. Success: (Yes|No)\. Elapsed time: ([\d.]+)"
    )

    entries = []
    for content in log_contents:
        matches = pattern.findall(content)
        for match in matches:
            user, lock, success, elapsed = match
            entries.append({
                "Nick": user,
                "Zamek": lock,
                "Sukces": success == "Yes",
                "Czas": float(elapsed)
            })

    print(f"✅ Liczba wpisów rozpoznanych: {len(entries)}\n")
    return entries


def create_dataframe(entries):
    print(f"[{datetime.now()}] 📊 Tworzenie tabeli wyników...")
    df = pd.DataFrame(entries)
    if df.empty:
        print("⚠️ Brak danych do analizy.\n")
        return ""

    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Proby=("Sukces", "count"),
        Udane=("Sukces", "sum"),
        Nieudane=("Sukces", lambda x: (~x).sum()),
        SredniCzas=("Czas", "mean")
    ).reset_index()

    grouped["Skuteczność"] = (grouped["Udane"] / grouped["Proby"] * 100).round(2)
    grouped["SredniCzas"] = grouped["SredniCzas"].round(2)

    grouped["SredniCzas"] = grouped["SredniCzas"].astype(str) + " s"
    grouped["Skuteczność"] = grouped["Skuteczność"].astype(str) + " %"

    tabela = tabulate(
        grouped,
        headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Średni czas", "Skuteczność"],
        tablefmt="github",
        stralign="center",
        numalign="center"
    )

    print("✅ Tabela gotowa:\n")
    print(tabela)
    return tabela


def send_to_discord(tabela):
    if not tabela:
        print("⚠️ Brak danych do wysłania na Discord.\n")
        return

    print(f"[{datetime.now()}] 🚀 Wysyłanie tabeli na Discord...")
    try:
        response = requests.post(WEBHOOK_URL, json={"content": f"```\n{tabela}\n```"})
        if response.status_code == 204:
            print("✅ Webhook wysłany poprawnie.\n")
        else:
            print(f"❌ Błąd webhooka: {response.status_code} - {response.text}\n")
    except Exception as e:
        print(f"❌ Wyjątek przy wysyłce: {e}\n")


def analyze_and_send_loop():
    print(f"🔁 Rozpoczynam automatyczny tryb przetwarzania co 60 sekund...\n")
    while True:
        try:
            log_data = fetch_log_files_from_ftp()
            parsed = parse_log_content(log_data)
            tabela = create_dataframe(parsed)
            send_to_discord(tabela)
        except Exception as e:
            print(f"❌ Błąd głównej pętli: {e}\n")
        time.sleep(60)


# ---------- Flask endpoint (info) ----------

@app.route("/")
def index():
    return "Lockpick Analyzer działa w tle i wysyła dane co 60s."


# ---------- Start aplikacji ----------

if __name__ == "__main__":
    # Uruchomienie pętli automatycznej w osobnym wątku
    thread = threading.Thread(target=analyze_and_send_loop)
    thread.daemon = True
    thread.start()

    # Flask tylko do testu działania — dostępne pod / w razie potrzeby
    app.run(host="0.0.0.0", port=10000)
