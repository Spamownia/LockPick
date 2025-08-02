import io
import re
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

# === POBIERANIE LOGÓW Z FTP ===
def ftp_get_logs():
    print("🔗 Łączenie z FTP (bez TLS)...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)
    print(f"📂 Przeglądanie katalogu: {FTP_LOG_PATH}")

    filenames = []
    def parse_line(line):
        # Przykład line: "-rw-r--r-- 1 user group 12345 Jul 28 10:00 gameplay_abc123.log"
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
        return ""

    log_texts = []
    for filename in filenames:
        print(f"⬇️ Pobieranie: {filename}")
        bio = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {filename}", bio.write)
            bio.seek(0)
            content = bio.read().decode("utf-16le")
            log_texts.append(content)
        except Exception as e:
            print(f"❌ Błąd podczas pobierania {filename}: {e}")

    ftp.quit()
    combined_logs = "\n".join(log_texts)
    print(f"📥 Wszystkie logi połączone. Rozmiar: {len(combined_logs)} znaków.")
    return combined_logs

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
        time = float(match.group("time"))
        lock = match.group("lock")
        print(f"  • Wpis #{count}: Użytkownik={user}, Sukces={success}, Czas={time}, Rodzaj zamka={lock}")
        entries.append((user, lock, success, time))
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

# === GŁÓWNA FUNKCJA ===
if __name__ == "__main__":
    logs = ftp_get_logs()
    if logs:
        parsed_entries = parse_log_minigame(logs)
        analyzed_df = analyze_data(parsed_entries)
        table_text = format_table(analyzed_df)
        send_to_discord(table_text)
    else:
        print("⚠️ Brak logów do przetworzenia.")
