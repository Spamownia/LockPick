import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
from tabulate import tabulate
from datetime import datetime
from flask import Flask
import threading
import requests

# --- Konfiguracja ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

CHECK_INTERVAL = 60
SEEN_LINES = set()

# --- Inicjalizacja Flask ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- Funkcje pomocnicze ---
def connect_ftp():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        print("[DEBUG] Połączono z FTP")
        return ftp
    except Exception as e:
        print(f"[ERROR] Błąd połączenia z FTP: {e}")
        return None

def list_log_files(ftp):
    try:
        files = []
        ftp.retrlines("LIST", lambda line: files.append(line))
        log_files = [line.split()[-1] for line in files if line.split()[-1].startswith("gameplay_") and line.endswith(".log")]
        print(f"[DEBUG] Lista plików logów: {log_files}")
        return log_files
    except Exception as e:
        print(f"[ERROR] Błąd listowania plików FTP: {e}")
        return []

def download_logs():
    ftp = connect_ftp()
    if ftp is None:
        return ""
    log_files = list_log_files(ftp)
    logs = []
    for filename in log_files:
        try:
            print(f"[DEBUG] Przetwarzanie pliku: {filename}")
            content = []
            ftp.retrbinary(f"RETR {filename}", lambda data: content.append(data))
            log_data = b"".join(content).decode("utf-16le", errors="ignore")
            logs.append(log_data)
            print(f"[DEBUG] Pobrano plik: {filename}, długość: {len(log_data)} znaków")
        except Exception as e:
            print(f"[ERROR] Błąd pobierania pliku {filename}: {e}")
    try:
        ftp.quit()
    except Exception as e:
        print(f"[WARNING] Błąd zamknięcia FTP: {e}")
    return "\n".join(logs)

def parse_log_content(log_text):
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>\w+) .*? Lock: (?P<lock>\w+).*? Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d.]+)",
        re.DOTALL
    )
    entries = []
    total_found = 0
    for match in pattern.finditer(log_text):
        total_found += 1
        raw_line = match.group(0)
        if raw_line in SEEN_LINES:
            print(f"[DEBUG] Pominięto powtórzony wpis: {raw_line}")
            continue
        print(f"[DEBUG] Rozpoznany nowy wpis: {raw_line}")
        SEEN_LINES.add(raw_line)
        entries.append({
            "Nick": match.group("nick"),
            "Zamek": match.group("lock"),
            "Sukces": match.group("success") == "Yes",
            "Czas": float(match.group("time"))
        })
    print(f"[DEBUG] Razem znalezionych wpisów: {total_found}, nowych: {len(entries)}")
    return entries

def save_to_db(entries):
    if not entries:
        print("[DEBUG] Brak wpisów do zapisu w bazie")
        return
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking (
                nick TEXT,
                zamek TEXT,
                sukces BOOLEAN,
                czas REAL
            )
        """)
        for entry in entries:
            cur.execute(
                "INSERT INTO lockpicking (nick, zamek, sukces, czas) VALUES (%s, %s, %s, %s)",
                (entry["Nick"], entry["Zamek"], entry["Sukces"], entry["Czas"])
            )
        conn.commit()
        print(f"[DEBUG] Zapisano {len(entries)} wpisów do bazy")
    except Exception as e:
        print(f"[ERROR] Błąd zapisu do bazy: {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

def create_dataframe():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql_query("SELECT * FROM lockpicking", conn)
        conn.close()
        if df.empty:
            print("[DEBUG] Brak danych w bazie do tworzenia tabeli")
            return None
        grouped = df.groupby(["nick", "zamek"]).agg(
            Proby=('sukces', 'count'),
            Udane=('sukces', 'sum'),
            Nieudane=('sukces', lambda x: (~x).sum()),
            SredniCzas=('czas', 'mean')
        ).reset_index()
        grouped['Skutecznosc'] = (grouped['Udane'] / grouped['Proby'] * 100).round(1)
        grouped['SredniCzas'] = grouped['SredniCzas'].round(2)
        print(f"[DEBUG] Utworzono DataFrame z {len(grouped)} wierszami")
        return grouped
    except Exception as e:
        print(f"[ERROR] Błąd tworzenia DataFrame: {e}")
        return None

def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania na Discord")
        return
    try:
        df.columns = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Średni czas", "Skuteczność"]
        for col in df.columns:
            df[col] = df[col].astype(str)
        tabela = tabulate(df.values.tolist(), headers=df.columns.tolist(), tablefmt="github", stralign="center", numalign="center")
        payload = {"content": f"```\n{tabela}\n```"}
        response = requests.post(WEBHOOK_URL, json=payload)
        print(f"[DEBUG] Wysłano dane do Discorda, status: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Błąd wysyłania na Discord: {e}")

def main_loop():
    print("[DEBUG] Start main_loop")
    while True:
        print(f"[DEBUG] --- Sprawdzanie logów: {datetime.utcnow().isoformat()} ---")
        log_text = download_logs()
        if not log_text:
            print("[DEBUG] Brak tekstu logów do przetworzenia")
            time.sleep(CHECK_INTERVAL)
            continue
        new_entries = parse_log_content(log_text)
        if new_entries:
            print(f"[DEBUG] Nowe wpisy: {len(new_entries)}")
            save_to_db(new_entries)
            df = create_dataframe()
            if df is not None:
                send_to_discord(df)
        else:
            print("[DEBUG] Brak nowych zdarzeń w logach")
        time.sleep(CHECK_INTERVAL)

# --- Uruchomienie wątku pętli głównej ---
threading.Thread(target=main_loop, daemon=True).start()

# --- Uruchomienie serwera Flask ---
app.run(host='0.0.0.0', port=3000)
