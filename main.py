import os
import ftplib
import io
import pandas as pd
from tabulate import tabulate
import requests
from flask import Flask

# === FLASK ===
app = Flask(__name__)

@app.route("/")
def index():
    return "OK"

# === USTAWIENIA FTP I WEBHOOK ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# === POBIERANIE LOGÓW ===
def fetch_log_files_from_ftp():
    logs = []
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)
        filenames = ftp.nlst()
        for filename in filenames:
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                print(f"Pobieranie pliku: {filename}")
                with io.BytesIO() as log_stream:
                    ftp.retrbinary(f"RETR {filename}", log_stream.write)
                    log_stream.seek(0)
                    content = log_stream.read().decode("utf-16-le", errors="ignore")
                    logs.append(content)
    return logs

# === PARSOWANIE LOGÓW ===
def parse_logs(log_contents):
    results = {}
    for content in log_contents:
        for line in content.splitlines():
            if "[LogMinigame]" in line and "LockpickingMinigame_C" in line:
                parts = line.split(" ")
                try:
                    user_index = parts.index("User:")
                    user = parts[user_index + 1]
                    lock_index = parts.index("Type:")
                    lock = parts[lock_index + 1]
                    success_index = parts.index("Success:")
                    success = parts[success_index + 1] == "Yes."
                    time_index = parts.index("Elapsed")
                    elapsed = float(parts[time_index + 2].replace("s", "").replace(".", "").strip()) / 100
                except (ValueError, IndexError):
                    continue

                key = (user, lock)
                if key not in results:
                    results[key] = {
                        "Nick": user,
                        "Rodzaj zamka": lock,
                        "Wszystkie": 0,
                        "Udane": 0,
                        "Nieudane": 0,
                        "Czas sumaryczny": 0.0
                    }

                results[key]["Wszystkie"] += 1
                if success:
                    results[key]["Udane"] += 1
                else:
                    results[key]["Nieudane"] += 1
                results[key]["Czas sumaryczny"] += elapsed
    return results

# === TWORZENIE TABELI CSV I WEBHOOK ===
def create_and_send_table(results):
    if not results:
        print("Brak danych do wysłania.")
        return

    rows = []
    for data in results.values():
        skutecznosc = (data["Udane"] / data["Wszystkie"]) * 100 if data["Wszystkie"] else 0
        sredni_czas = data["Czas sumaryczny"] / data["Wszystkie"] if data["Wszystkie"] else 0
        rows.append({
            "Nick": data["Nick"],
            "Rodzaj zamka": data["Rodzaj zamka"],
            "Wszystkie": data["Wszystkie"],
            "Udane": data["Udane"],
            "Nieudane": data["Nieudane"],
            "Skuteczność": f"{skutecznosc:.2f}%",
            "Średni czas": f"{sredni_czas:.2f} s"
        })

    df = pd.DataFrame(rows)
    df.sort_values(by=["Nick", "Rodzaj zamka"], inplace=True)
    df_formatted = tabulate(df, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    
    print("Tabela:\n", df_formatted)

    payload = {
        "content": f"```{df_formatted}```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code != 204:
        print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")
    else:
        print("✅ Tabela została wysłana na webhook.")

# === FUNKCJA GŁÓWNA ===
def run():
    print("🔄 Rozpoczynanie pobierania logów z FTP...")
    log_data = fetch_log_files_from_ftp()
    print(f"📄 Ilość plików: {len(log_data)}")
    parsed = parse_logs(log_data)
    create_and_send_table(parsed)

# === START FLASKA ORAZ WYWOŁANIE LOGIKI ===
if __name__ == "__main__":
    run()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
