import ftplib
import io
import re
import pandas as pd
import requests
from tabulate import tabulate

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOGS_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- REGEX DOPASOWUJĄCY WPISY LOGÓW ---
LOG_ENTRY_REGEX = re.compile(
    r"\[LogMinigame\].*?User:\s+(?P<nick>\w+)\s+\(\d+,\s+\d+\)\.\s+Success:\s+(?P<success>Yes|No)\.\s+Elapsed time:\s+(?P<time>\d+\.\d+)\.\s+Failed attempts:\s+\d+\.\s+Target object:.*?\.\s+Lock type:\s+(?P<lock_type>\w+)\.",
    re.DOTALL
)

# --- FUNKCJE ---

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    return ftp

def download_log_files():
    ftp = connect_ftp()
    ftp.cwd(LOGS_DIR)
    filenames = []
    ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
    log_files = [name for name in filenames if name.startswith("gameplay_") and name.endswith(".log")]

    logs_data = []
    for filename in log_files:
        try:
            with io.BytesIO() as bio:
                ftp.retrbinary(f"RETR {filename}", bio.write)
                bio.seek(0)
                content = bio.read().decode("utf-16-le", errors="ignore")
                logs_data.append((filename, content))
                print(f"[INFO] Pobrano: {filename}")
                print(f"[DEBUG] Fragment logu ({filename}): {content[:300]!r} ...")
        except Exception as e:
            print(f"[BŁĄD] Nie udało się pobrać {filename}: {e}")
    ftp.quit()
    print(f"[DEBUG] Znaleziono plików: {len(log_files)}")
    return logs_data

def parse_log_content(content):
    entries = []
    for match in LOG_ENTRY_REGEX.finditer(content):
        nick = match.group("nick")
        success = match.group("success") == "Yes"
        elapsed_time = float(match.group("time"))
        lock_type = match.group("lock_type")
        entries.append({
            "Nick": nick,
            "Zamek": lock_type,
            "Udane": int(success),
            "Nieudane": int(not success),
            "Czas": elapsed_time
        })
    return entries

def create_dataframe(entries):
    if not entries:
        return pd.DataFrame()
    df = pd.DataFrame(entries)
    grouped = df.groupby(["Nick", "Zamek"], as_index=False).agg(
        Wszystkie=pd.NamedAgg(column="Udane", aggfunc="count"),
        Udane=pd.NamedAgg(column="Udane", aggfunc="sum"),
        Nieudane=pd.NamedAgg(column="Nieudane", aggfunc="sum"),
        Skuteczność=pd.NamedAgg(column="Udane", aggfunc=lambda x: round(100 * x.sum() / x.count(), 2)),
        Średni_czas=pd.NamedAgg(column="Czas", aggfunc="mean")
    )
    grouped["Średni_czas"] = grouped["Średni_czas"].round(2)
    return grouped

def send_to_discord(df):
    if df.empty:
        print("[INFO] Brak danych do wysłania.")
        return
    # Przygotowanie tabeli do wysłania na Discord - wyśrodkowane kolumny i dopasowane szerokości
    table = tabulate(
        df,
        headers=["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"],
        tablefmt="github",
        colalign=("center",)*7
    )
    print("[DEBUG] Tabela do wysłania na Discord:\n")
    print(table)
    payload = {"content": f"```\n{table}\n```"}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            print("[OK] Wysłano dane na Discord webhook.")
        else:
            print(f"[BŁĄD] Webhook zwrócił status: {response.status_code}")
    except Exception as e:
        print(f"[BŁĄD] Błąd przy wysyłaniu na webhook: {e}")

def main():
    print("[DEBUG] Start programu")
    logs = download_log_files()
    all_entries = []
    for filename, content in logs:
        entries = parse_log_content(content)
        print(f"[DEBUG] Przetworzono {len(entries)} wpisów z pliku: {filename}")
        all_entries.extend(entries)
    print(f"[DEBUG] Wszystkich wpisów: {len(all_entries)}")

    df = create_dataframe(all_entries)
    send_to_discord(df)

if __name__ == "__main__":
    main()
