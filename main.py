import re
import pandas as pd
import ftplib
import io
import psycopg2
from tabulate import tabulate
import requests
from datetime import datetime
import time

# --- Konfiguracja FTP ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOGS_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# --- Konfiguracja bazy danych ---
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- Webhook Discord ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Regex do parsowania linii ---
LOG_LINE_REGEX = re.compile(
    r"User: (?P<user_name>.+?) \((?P<user_id>\d+), (?P<user_steam_id>\d+)\)\. "
    r"Success: (?P<success>Yes|No)\. "
    r"Elapsed time: (?P<elapsed_time>[\d\.]+)\. "
    r"Failed attempts: (?P<failed_attempts>\d+)\. "
    r"Target object: (?P<target_object>.+?)\(ID: (?P<target_id>.+?)\)\. "
    r"Lock type: (?P<lock_type>\w+)\. "
    r"User owner: (?P<user_owner>.+?)\. "
    r"Location: X=(?P<x>-?[\d\.]+) Y=(?P<y>-?[\d\.]+) Z=(?P<z>-?[\d\.]+)"
)

def parse_log_content(log_content: str):
    """
    Parsuje zawartość logu i zwraca listę słowników z danymi.
    Zabezpiecza przed błędami indeksów przez stosowanie regex.
    """
    parsed_entries = []
    lines = log_content.splitlines()
    for line in lines:
        if "[LockpickingMinigame_C]" in line:
            try:
                match = LOG_LINE_REGEX.search(line)
                if not match:
                    print(f"⚠️  Błąd podczas parsowania linii (niepasujący format): {line[:100]}...")
                    continue

                data = match.groupdict()

                entry = {
                    "User": data["user_name"].strip(),
                    "UserID": int(data["user_id"]),
                    "UserSteamID": data["user_steam_id"],
                    "Success": data["success"] == "Yes",
                    "ElapsedTime": float(data["elapsed_time"]),
                    "FailedAttempts": int(data["failed_attempts"]),
                    "TargetObject": data["target_object"].strip(),
                    "TargetID": data["target_id"].strip(),
                    "LockType": data["lock_type"],
                    "UserOwner": data["user_owner"].strip(),
                    "LocationX": float(data["x"]),
                    "LocationY": float(data["y"]),
                    "LocationZ": float(data["z"]),
                }
                parsed_entries.append(entry)
            except Exception as e:
                print(f"⚠️  Błąd podczas parsowania linii: {line[:100]}... Exception: {e}")
    return parsed_entries

def download_logs_from_ftp():
    print("🔄 Łączenie z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOGS_PATH)
    print("📂 Pobieram listę plików...")
    files = ftp.nlst()
    log_files = sorted([f for f in files if f.startswith("gameplay_") and f.endswith(".log")])
    print(f"📄 Znaleziono {len(log_files)} plików logów.")
    
    all_entries = []
    for filename in log_files:
        try:
            print(f"📥 Pobieram plik: {filename}")
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            bio.seek(0)
            content = bio.read().decode("utf-16le")
            parsed = parse_log_content(content)
            print(f"✅ Przetworzono plik: {filename} ({len(parsed)} wpisów)")
            all_entries.extend(parsed)
        except Exception as e:
            print(f"⚠️ Błąd pobierania lub parsowania pliku {filename}: {e}")
    ftp.quit()
    return all_entries

def create_dataframe(entries):
    if not entries:
        return pd.DataFrame()
    df = pd.DataFrame(entries)
    # Agregacja i sumowanie statystyk per User i LockType
    summary = df.groupby(["User", "LockType"]).agg(
        TotalAttempts=pd.NamedAgg(column="Success", aggfunc="count"),
        SuccessfulAttempts=pd.NamedAgg(column="Success", aggfunc="sum"),
        FailedAttempts=pd.NamedAgg(column="FailedAttempts", aggfunc="sum"),
        AverageElapsedTime=pd.NamedAgg(column="ElapsedTime", aggfunc="mean"),
    ).reset_index()

    # Obliczanie skuteczności
    summary["SuccessRate"] = (summary["SuccessfulAttempts"] / summary["TotalAttempts"]) * 100
    # Zaokrąglenia
    summary["AverageElapsedTime"] = summary["AverageElapsedTime"].round(2)
    summary["SuccessRate"] = summary["SuccessRate"].round(2)

    return summary

def send_to_discord(df):
    if df.empty:
        print("ℹ️  Brak danych do wysłania na Discord.")
        return

    # Tworzenie tabeli tekstowej z wyśrodkowaniem
    table = tabulate(
        df,
        headers="keys",
        tablefmt="pretty",
        showindex=False,
        stralign="center"
    )
    data = {
        "content": f"```{table}```"
    }
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code == 204:
        print("✅ Dane wysłane do Discord.")
    else:
        print(f"⚠️ Błąd wysyłki na Discord: {response.status_code} {response.text}")

def main_loop():
    while True:
        print(f"[{datetime.now()}] 🔄 Pobieranie logów z FTP...")
        entries = download_logs_from_ftp()
        df = create_dataframe(entries)
        send_to_discord(df)
        print("🔁 Czekam 60 sekund przed kolejnym przetwarzaniem...")
        time.sleep(60)

if __name__ == "__main__":
    main_loop()
