import io
import re
import time
import requests
import pandas as pd
from ftplib import FTP
from tabulate import tabulate
from flask import Flask
import threading

# === KONFIGURACJA ===

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

CHECK_INTERVAL = 60  # sekundy

app = Flask(__name__)
seen_entries = set()  # unikalne wpisy, aby nie duplikować
last_filename = None

# === FUNKCJE POMOCNICZE ===

def connect_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)
    return ftp

def get_log_filenames(ftp):
    filenames = []
    def parse_line(line):
        parts = line.split()
        if len(parts) >= 9:
            fname = parts[-1]
            if fname.startswith("gameplay_") and fname.endswith(".log"):
                filenames.append(fname)
    ftp.retrlines('LIST', parse_line)
    return sorted(filenames)

def read_log_file(ftp, filename):
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    bio.seek(0)
    return bio.read().decode("utf-16le")

def parse_log_minigame(log_text):
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\].*?User: (?P<user>\w+).*?Success: (?P<success>Yes|No).*?Elapsed time: (?P<time>[\d.]+).*?Lock type: (?P<lock>\w+)",
        re.DOTALL
    )
    entries = []
    for match in pattern.finditer(log_text):
        raw_line = match.group(0)
        if raw_line not in seen_entries:
            seen_entries.add(raw_line)
            user = match.group("user")
            success = match.group("success") == "Yes"
            time_taken = float(match.group("time"))
            lock_type = match.group("lock")
            entries.append((user, lock_type, success, time_taken))
    return entries

def analyze_data(entries):
    df = pd.DataFrame(entries, columns=["Nick", "Zamek", "Sukces", "Czas"])
    if df.empty:
        return pd.DataFrame()
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

def format_table(df):
    if df.empty:
        return "Brak nowych danych do wyświetlenia."
    table = tabulate(df.values, headers=df.columns, tablefmt="github", stralign="center", numalign="center")
    return f"\n{table}\n"

def send_to_discord(content):
    response = requests.post(WEBHOOK_URL, json={"content": content})
    if response.status_code not in (200, 204):
        print(f"❌ Błąd wysyłania na Discord: {response.status_code} – {response.text}")

# === GŁÓWNA PĘTLA ===

def monitor_loop():
    global last_filename
    print("🚀 Start bota LockpickingLogger...")

    # Initial scan
    try:
        print("🔍 Skanowanie wszystkich logów przy starcie...")
        ftp = connect_ftp()
        all_files = get_log_filenames(ftp)
        for fname in all_files:
            content = read_log_file(ftp, fname)
            entries = parse_log_minigame(content)
        ftp.quit()
        print(f"✅ Wczytano i zindeksowano {len(seen_entries)} unikalnych wpisów.")
    except Exception as e:
        print(f"❌ Błąd FTP (startup): {e}")

    # Loop
    while True:
        try:
            ftp = connect_ftp()
            filenames = get_log_filenames(ftp)
            if not filenames:
                print("⚠️ Brak plików logów.")
                time.sleep(CHECK_INTERVAL)
                continue
            latest_file = filenames[-1]
            if last_filename != latest_file:
                print(f"📁 Nowy plik logów: {latest_file}")
                last_filename = latest_file
            content = read_log_file(ftp, latest_file)
            ftp.quit()
            new_entries = parse_log_minigame(content)
            if new_entries:
                df = analyze_data(new_entries)
                table = format_table(df)
                send_to_discord(table)
            else:
                print("⏱️ Brak nowych wpisów.")
        except Exception as e:
            print(f"❌ Błąd monitorowania FTP: {e}")
        time.sleep(CHECK_INTERVAL)

# === FLASK KEEP-ALIVE ===

@app.route("/")
def index():
    return "LockpickingLogger działa 🚀"

if __name__ == "__main__":
    threading.Thread(target=monitor_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
