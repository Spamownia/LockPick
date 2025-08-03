import io
import re
import time
import threading
import requests
import pandas as pd
from ftplib import FTP
from tabulate import tabulate
from flask import Flask

# === KONFIGURACJA ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

last_offset = 0
last_filename = None

# === FLASK (utrzymanie działania) ===
app = Flask(__name__)

@app.route("/")
def index():
    return "", 200  # Pusty 200 OK

# === FTP: NAJNOWSZY PLIK ===
def get_latest_log_file():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)

    filenames = []

    def parse_line(line):
        parts = line.split()
        if len(parts) >= 9:
            name = parts[-1]
            if name.startswith("gameplay_") and name.endswith(".log"):
                filenames.append(name)

    ftp.retrlines('LIST', parse_line)

    if not filenames:
        ftp.quit()
        return None, None

    latest = sorted(filenames)[-1]
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {latest}", bio.write)
    ftp.quit()
    bio.seek(0)
    return latest, bio.read().decode("utf-16le", errors="ignore")

# === FTP: NOWE DANE Z OFFSETU ===
def get_new_log_data(filename, start_offset):
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)
    bio = io.BytesIO()
    def handle_binary(data):
        bio.write(data)
    try:
        ftp.sendcmd("TYPE I")
        size = ftp.size(filename)
        if start_offset >= size:
            ftp.quit()
            return "", start_offset
        ftp.retrbinary(f"RETR {filename}", callback=handle_binary, rest=start_offset)
    except Exception as e:
        print(f"❌ Błąd FTP: {e}")
        ftp.quit()
        return "", start_offset
    ftp.quit()
    bio.seek(0)
    return bio.read().decode("utf-16le", errors="ignore"), size

# === PARSOWANIE ===
def parse_log_minigame(log_text):
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<user>\w+).*?Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d\.]+)\..*?Lock type: (?P<lock>\w+)\.",
        re.DOTALL
    )
    entries = []
    for match in pattern.finditer(log_text):
        user = match.group("user")
        success = match.group("success") == "Yes"
        time_taken = float(match.group("time"))
        lock_type = match.group("lock")
        entries.append((user, lock_type, success, time_taken))
    return entries

# === ANALIZA DANYCH ===
def analyze_data(entries):
    if not entries:
        return pd.DataFrame()
    df = pd.DataFrame(entries, columns=["Nick", "Zamek", "Sukces", "Czas"])
    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Wszystkie=("Sukces", "count"),
        Udane=("Sukces", "sum"),
        Nieudane=("Sukces", lambda x: (~x).sum()),
        Średni_czas=("Czas", "mean"),
    )
    grouped["Skuteczność"] = (grouped["Udane"] / grouped["Wszystkie"] * 100).round(1).astype(str) + "%"
    grouped["Średni_czas"] = grouped["Średni_czas"].round(2).astype(str) + "s"
    grouped = grouped.reset_index().sort_values(by=["Nick", "Zamek"])
    return grouped[["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni_czas"]]

# === FORMATOWANIE TABELI ===
def format_table(df):
    if df.empty:
        return None
    return "```\n" + tabulate(
        df.values,
        headers=df.columns,
        tablefmt="github",
        stralign="center",
        numalign="center"
    ) + "\n```"

# === WYSYŁKA NA DISCORDA ===
def send_to_discord(content):
    response = requests.post(WEBHOOK_URL, json={"content": content})
    if response.status_code in (200, 204):
        print("✅ Wysłano na Discord.")
    else:
        print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")

# === PĘTLA BOTA ===
def bot_loop():
    global last_filename, last_offset
    print("🚀 Start bota LockpickingLogger...")
    last_filename, full_log = get_latest_log_file()
    if not last_filename:
        print("❌ Brak plików logów do analizy.")
        return

    print(f"📁 Ostatni log: {last_filename}")
    parsed = parse_log_minigame(full_log)
    last_offset = len(full_log.encode("utf-16le"))
    analyzed = analyze_data(parsed)
    msg = format_table(analyzed)
    if msg:
        send_to_discord(msg)

    while True:
        time.sleep(60)
        print(f"🔄 Sprawdzanie nowości w {last_filename}...")
        new_data, new_offset = get_new_log_data(last_filename, last_offset)
        if new_data:
            new_entries = parse_log_minigame(new_data)
            if new_entries:
                analyzed = analyze_data(new_entries)
                msg = format_table(analyzed)
                if msg:
                    send_to_discord(msg)
                else:
                    print("ℹ️ Brak nowych wpisów do wysyłki.")
            else:
                print("ℹ️ Nie znaleziono nowych prób.")
            last_offset = new_offset
        else:
            print("⏳ Brak nowych danych.")

# === START ===
if __name__ == "__main__":
    threading.Thread(target=bot_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
