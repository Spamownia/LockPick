import io
import re
import time
import threading
import requests
import pandas as pd
from ftplib import FTP
from tabulate import tabulate
from flask import Flask

# ====== KONFIGURACJA ======
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Zmienna globalna do zapamiętania ostatniej wielkości odczytanego pliku
last_log_position = 0
last_log_filename = None

def ftp_get_log_filenames():
    print("🔗 Łączenie z FTP (bez TLS)...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)
    filenames = []
    def parse_line(line):
        parts = line.split()
        if len(parts) >= 9:
            fname = parts[-1]
            if fname.startswith("gameplay_") and fname.endswith(".log"):
                filenames.append(fname)
    try:
        ftp.retrlines('LIST', parse_line)
    except Exception as e:
        print(f"❌ Błąd pobierania listy plików: {e}")
    ftp.quit()
    filenames.sort()
    return filenames

def ftp_read_new_log_data(filename, from_pos):
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)
    bio = io.BytesIO()
    try:
        ftp.retrbinary(f"RETR {filename}", bio.write)
        bio.seek(from_pos)
        new_data = bio.read().decode("utf-16le")
    except Exception as e:
        print(f"❌ Błąd pobierania pliku {filename}: {e}")
        new_data = ""
    ftp.quit()
    return new_data

def parse_log_minigame(log_text):
    print("🧠 Parsowanie wpisów z [LogMinigame]...")
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\].*?User: (?P<user>\w+).*?Success: (?P<success>Yes|No).*?Elapsed time: (?P<time>[\d.]+).*?Lock type: (?P<lock>\w+)",
        re.DOTALL
    )
    entries = []
    count = 0
    for match in pattern.finditer(log_text):
        count += 1
        user = match.group("user")
        success = match.group("success") == "Yes"
        time_val = float(match.group("time"))
        lock = match.group("lock")
        print(f"  • Wpis #{count}: Użytkownik={user}, Sukces={success}, Czas={time_val}, Rodzaj zamka={lock}")
        entries.append((user, lock, success, time_val))
    print(f"✅ Parsowanie zakończone, znaleziono {count} wpisów.")
    return entries

def analyze_data(entries):
    print("📊 Analizuję dane...")
    df = pd.DataFrame(entries, columns=["Nick", "Zamek", "Sukces", "Czas"])
    if df.empty:
        print("⚠️ Brak danych do analizy.")
        return pd.DataFrame()
    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Wszystkie=("Sukces", "count"),
        Udane=("Sukces", "sum"),
        Nieudane=("Sukces", lambda x: (~x).sum()),
        Średni_czas=("Czas", "mean"),
    )
    grouped["Skuteczność"] = (grouped["Udane"] / grouped["Wszystkie"] * 100).round(1).astype(str) + "%"
    grouped["Średni_czas"] = grouped["Średni_czas"].round(2).astype(str) + "s"
    grouped = grouped.reset_index()
    grouped = grouped.sort_values(by=["Nick", "Zamek"])
    print("✅ Analiza zakończona. Oto podsumowanie:")
    print(grouped)
    return grouped[["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni_czas"]]

def format_table(df):
    print("📝 Tworzę tabelę markdown z wyśrodkowaniem...")
    if df.empty:
        return "Brak danych do wyświetlenia."
    table = tabulate(
        df.values,
        headers=df.columns,
        tablefmt="github",
        stralign="center",
        numalign="center"
    )
    print("Tabela gotowa.")
    return f"\n{table}\n"

def send_to_discord(content):
    print("🚀 Wysyłam tabelę na Discord webhook...")
    response = requests.post(WEBHOOK_URL, json={"content": content})
    if response.status_code in (200, 204):
        print("✅ Wysłano pomyślnie.")
    else:
        print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")

def monitor_loop():
    global last_log_position, last_log_filename
    print("🔍 Skanowanie wszystkich logów przy starcie...")
    filenames = ftp_get_log_filenames()
    if not filenames:
        print("⚠️ Nie znaleziono plików logów.")
    else:
        last_log_filename = filenames[-1]
        print(f"📄 Ostatni log: {last_log_filename}")
        # Przy starcie pobierz całość ostatniego logu
        full_log = ftp_read_new_log_data(last_log_filename, 0)
        last_log_position = len(full_log.encode("utf-16le"))
        print(f"📥 Pobranie pełnej zawartości ostatniego logu (rozmiar w bajtach): {last_log_position}")
        entries = parse_log_minigame(full_log)
        if entries:
            df = analyze_data(entries)
            table = format_table(df)
            send_to_discord(table)

    while True:
        try:
            print("⏰ Cykl monitorowania - łączenie z FTP...")
            filenames = ftp_get_log_filenames()
            if not filenames:
                print("⚠️ Nie znaleziono plików logów w trakcie monitoringu.")
                time.sleep(60)
                continue
            current_log = filenames[-1]
            if current_log != last_log_filename:
                # Nowy plik logu pojawił się
                print(f"🆕 Wykryto nowy plik logu: {current_log}")
                last_log_filename = current_log
                last_log_position = 0
            new_data = ftp_read_new_log_data(last_log_filename, last_log_position)
            new_bytes_len = len(new_data.encode("utf-16le"))
            if new_bytes_len > 0:
                print(f"⬇️ Nowe dane w logu {last_log_filename}: {new_bytes_len} bajtów")
                last_log_position += new_bytes_len
                entries = parse_log_minigame(new_data)
                if entries:
                    df = analyze_data(entries)
                    table = format_table(df)
                    send_to_discord(table)
            else:
                print("⏳ Brak nowych danych w logu.")
        except Exception as e:
            print(f"❌ Błąd monitoringu: {e}")
        time.sleep(60)

app = Flask(__name__)

@app.route("/")
def home():
    return "LockpickingLogger is running."

if __name__ == "__main__":
    print("🚀 Start bota LockpickingLogger...")
    thread = threading.Thread(target=monitor_loop, daemon=True)
    thread.start()
    app.run(host="0.0.0.0", port=8080)
