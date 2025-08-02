import io
import re
import time
import requests
import pandas as pd
from ftplib import FTP
from tabulate import tabulate

# === KONFIGURACJA ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
CHECK_INTERVAL = 60  # sekund

# === POBIERANIE NAZW PLIKÓW Z FTP (gameplay_*.log) ===
def ftp_list_gameplay_logs(ftp):
    filenames = []
    def parse_line(line):
        parts = line.split()
        if len(parts) >= 9:
            fname = parts[-1]
            if fname.startswith("gameplay_") and fname.endswith(".log"):
                filenames.append(fname)
    ftp.retrlines('LIST', parse_line)
    return filenames

# === POBIERANIE JEDNEGO PLIKU LOGA Z FTP ===
def ftp_get_single_log(ftp, filename):
    print(f"⬇️ Pobieranie pliku: {filename}")
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    bio.seek(0)
    content = bio.read().decode("utf-16le")
    return content

# === PARSOWANIE LOGÓW ===
def parse_log_minigame(log_text):
    print("🧠 Parsowanie wpisów z [LogMinigame]...")
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<user>\w+).*?Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d\.]+)\..*?Lock type: (?P<lock>\w+)\.",
        re.DOTALL
    )
    entries = []
    count = 0
    for match in pattern.finditer(log_text):
        count += 1
        user = match.group("user")
        success = match.group("success") == "Yes"
        time_ = float(match.group("time"))
        lock = match.group("lock")
        print(f"  • Wpis #{count}: Użytkownik={user}, Sukces={success}, Czas={time_}, Rodzaj zamka={lock}")
        entries.append((user, lock, success, time_))
    print(f"✅ Parsowanie zakończone, znaleziono {count} wpisów.")
    return entries

# === ANALIZA DANYCH ===
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

# === FORMATOWANIE TABELI ===
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
    return f"```\n{table}\n```"

# === WYSYŁKA NA DISCORDA ===
def send_to_discord(content):
    print("🚀 Wysyłam tabelę na Discord webhook...")
    response = requests.post(WEBHOOK_URL, json={"content": content})
    if response.status_code in (200, 204):
        print("✅ Wysłano pomyślnie.")
    else:
        print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")

# === GŁÓWNA PĘTLA ===
def main_loop():
    last_processed_file = None
    while True:
        try:
            print("🔗 Łączenie z FTP (bez TLS)...")
            ftp = FTP()
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOG_PATH)

            filenames = ftp_list_gameplay_logs(ftp)
            if not filenames:
                print("⚠️ Nie znaleziono plików gameplay_*.log.")
                ftp.quit()
                time.sleep(CHECK_INTERVAL)
                continue

            # Sortujemy alfabetycznie i wybieramy ostatni plik (najświeższy)
            filenames.sort()
            newest_file = filenames[-1]

            if newest_file != last_processed_file:
                print(f"🔄 Nowy plik do przetworzenia: {newest_file}")
                log_content = ftp_get_single_log(ftp, newest_file)
                parsed_entries = parse_log_minigame(log_content)
                analyzed_df = analyze_data(parsed_entries)
                table_text = format_table(analyzed_df)
                send_to_discord(table_text)
                last_processed_file = newest_file
            else:
                print(f"ℹ️ Brak nowych plików od ostatniego sprawdzenia. Ostatni przetworzony: {last_processed_file}")

            ftp.quit()
        except Exception as e:
            print(f"❌ Błąd w pętli głównej: {e}")

        print(f"⏳ Czekam {CHECK_INTERVAL} sekund przed kolejnym sprawdzeniem...\n")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main_loop()
