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
    ftps = FTP()
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.login(FTP_USER, FTP_PASS)
    ftps.cwd(FTP_LOG_PATH)
    print(f"📂 Przeglądanie katalogu: {FTP_LOG_PATH}")
    
    try:
        files = ftps.nlst()
    except Exception as e:
        print(f"❌ Błąd pobierania listy plików: {e}")
        ftps.quit()
        return ""
    
    log_texts = []
    for filename in files:
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            print(f"⬇️ Pobieranie: {filename}")
            bio = io.BytesIO()
            try:
                ftps.retrbinary(f"RETR {filename}", bio.write)
                bio.seek(0)
                content = bio.read().decode("utf-16le")
                log_texts.append(content)
            except Exception as e:
                print(f"❌ Błąd podczas pobierania {filename}: {e}")
    ftps.quit()

    combined_logs = "\n".join(log_texts)
    print(f"📥 Wszystkie logi połączone. Rozmiar: {len(combined_logs)} znaków.")
    return combined_logs

# === PARSOWANIE LOGÓW ===
def parse_log_minigame(log_data):
    print("🧠 Parsowanie wpisów z [LogMinigame]...")
    pattern = re.compile(
        r"\[LogMinigame].*?User:\s*(?P<nick>.*?)\s+.*?"
        r"Type:\s*(?P<lock_type>.*?)\s+.*?"
        r"Success:\s*(?P<success>Yes|No).*?"
        r"Elapsed time:\s*(?P<elapsed>[0-9.]+)", re.DOTALL)

    results = []
    for match in pattern.finditer(log_data):
        results.append({
            "Nick": match.group("nick").strip(),
            "Zamek": match.group("lock_type").strip(),
            "Sukces": match.group("success") == "Yes",
            "Czas": float(match.group("elapsed")),
        })

    print(f"✅ Rozpoznano {len(results)} wpisów minigry.")
    return results

# === ANALIZA DANYCH ===
def analyze_data(entries):
    print("📊 Analiza danych...")
    df = pd.DataFrame(entries)
    if df.empty:
        print("⚠️ Brak danych do analizy.")
        return pd.DataFrame()

    grouped = df.groupby(["Nick", "Zamek"])
    summary = grouped.agg(
        **{
            "Ilość wszystkich prób": ("Sukces", "count"),
            "Udane": ("Sukces", "sum"),
            "Nieudane": (lambda x: (~x).sum()),
            "Skuteczność": ("Sukces", lambda x: f"{(x.mean()*100):.1f}%"),
            "Średni czas": ("Czas", lambda x: f"{x.mean():.2f}s")
        }
    ).reset_index()

    print("✅ Analiza zakończona.")
    return summary

# === FORMATOWANIE TABELI ===
def format_table(df):
    print("📐 Formatowanie tabeli do wysyłki...")
    if df.empty:
        return "Brak danych do wyświetlenia."

    table = tabulate(df, headers="keys", tablefmt="grid", showindex=False, stralign="center", numalign="center")
    return f"```\n{table}\n```"

# === WYSYŁKA NA DISCORDA ===
def send_to_discord(formatted_table):
    print("📤 Wysyłanie na Discord...")
    data = {"content": formatted_table}
    response = requests.post(WEBHOOK_URL, json=data)

    if response.status_code == 204:
        print("✅ Wysłano pomyślnie.")
    else:
        print(f"❌ Błąd wysyłania: {response.status_code} {response.text}")

# === GŁÓWNE WYKONANIE ===
if __name__ == "__main__":
    logs = ftp_get_logs()
    if logs:
        parsed_entries = parse_log_minigame(logs)
        summary = analyze_data(parsed_entries)
        formatted = format_table(summary)
        send_to_discord(formatted)
