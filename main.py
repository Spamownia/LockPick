import re
import requests
import pandas as pd
from tabulate import tabulate
from ftplib import FTP_TLS
import io

# 🔐 Dane logowania FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# 📬 Discord webhook
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

def ftp_get_logs():
    print("🔗 Łączenie z FTP...")
    ftps = FTP_TLS()
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.login(FTP_USER, FTP_PASS)
    ftps.prot_p()
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

def parse_log_minigame(log_text):
    print("🔍 Parsowanie logów LockpickingMinigame...")
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<user>\w+).*?Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d\.]+)\..*?Lock type: (?P<lock>\w+)\.",
        re.DOTALL
    )
    matches = pattern.finditer(log_text)
    data = []
    count = 0
    for match in matches:
        count += 1
        user = match.group("user")
        success = match.group("success") == "Yes"
        time = float(match.group("time"))
        lock = match.group("lock")
        print(f"  • Wpis #{count}: {user} | {lock} | {'✔' if success else '✘'} | {time:.2f}s")
        data.append((user, lock, success, time))
    print(f"✅ Zakończono parsowanie. Znaleziono {count} wpisów.")
    return data

def analyze_data(entries):
    print("📊 Analiza danych...")
    df = pd.DataFrame(entries, columns=["Nick", "Zamek", "Sukces", "Czas"])
    grouped = df.groupby(["Nick", "Zamek"], as_index=False).agg({
        "Sukces": ["count", "sum", lambda x: (~x).sum()],
        "Czas": "mean"
    })

    # 🧾 Korekta nazw kolumn
    grouped.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Średni czas"]
    grouped["Skuteczność"] = (grouped["Udane"] / grouped["Ilość wszystkich prób"] * 100).round(1).astype(str) + "%"
    grouped["Średni czas"] = grouped["Średni czas"].round(2).astype(str) + "s"

    # 🔢 Zmiana kolejności kolumn
    final = grouped[["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]]
    final = final.sort_values(by=["Nick", "Zamek"])
    print("✅ Analiza zakończona.")
    return final

def format_table(df):
    print("📝 Formatowanie tabeli do Discorda...")
    table = tabulate(
        df.values,
        headers=df.columns,
        tablefmt="github",
        stralign="center",
        numalign="center"
    )
    print("✅ Tabela gotowa.")
    return f"```\n{table}\n```"

def send_to_discord(content):
    print("🚀 Wysyłanie tabeli na Discord...")
    try:
        response = requests.post(WEBHOOK_URL, json={"content": content})
        if response.status_code in (200, 204):
            print("✅ Wysłano pomyślnie.")
        else:
            print(f"❌ Błąd: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Wyjątek podczas wysyłania: {e}")

if __name__ == "__main__":
    logs = ftp_get_logs()
    if logs:
        parsed_entries = parse_log_minigame(logs)
        if parsed_entries:
            df = analyze_data(parsed_entries)
            table = format_table(df)
            send_to_discord(table)
        else:
            print("⚠️ Brak wpisów LockpickingMinigame do analizy.")
    else:
        print("⚠️ Nie pobrano żadnych logów.")
