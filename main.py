import os
import re
import pandas as pd
import psycopg2
from io import StringIO
from tabulate import tabulate
from flask import Flask
from ftplib import FTP_TLS
import ssl
import requests

# --- Flask ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- Konfiguracja FTP i bazy ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Funkcje pomocnicze ---
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def fetch_ftp_log_files():
    print("[DEBUG] Nawiązywanie połączenia z FTP...")
    ftps = FTP_TLS()
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.auth()
    ftps.prot_p()
    ftps.login(FTP_USER, FTP_PASS)
    ftps.cwd(LOG_DIR)
    print("[DEBUG] Połączono z FTP, katalog:", LOG_DIR)
    filenames = []
    ftps.retrlines('LIST', lambda line: filenames.append(line.split()[-1]))
    log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików gameplay_*.log")
    return log_files, ftps

def download_and_decode_log(ftps, filename):
    print(f"[DEBUG] Pobieranie pliku: {filename}")
    content = []
    ftps.retrbinary(f"RETR {filename}", lambda data: content.append(data))
    raw_bytes = b"".join(content)
    try:
        return raw_bytes.decode("utf-16-le")
    except UnicodeDecodeError as e:
        print(f"[ERROR] Błąd dekodowania {filename}: {e}")
        return ""

def parse_log_content(content):
    print("[DEBUG] Parsowanie logów...")
    pattern = re.compile(
        r"\[LogMinigame\].*?User: (?P<user>.+?) \| Type: (?P<type>.+?) \| Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>\d+\.\d+)"
    )
    entries = []
    for match in pattern.finditer(content):
        user = match.group("user")
        lock_type = match.group("type")
        success = match.group("success") == "Yes"
        time = float(match.group("time"))
        entries.append((user, lock_type, success, time))
    print(f"[DEBUG] Rozpoznano {len(entries)} wpisów.")
    return entries

def insert_entries_to_db(entries):
    if not entries:
        print("[DEBUG] Brak danych do zapisania w bazie.")
        return

    with connect_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_stats (
                nick TEXT,
                lock_type TEXT,
                success BOOLEAN,
                time FLOAT
            )
        """)
        insert_query = "INSERT INTO lockpicking_stats (nick, lock_type, success, time) VALUES (%s, %s, %s, %s)"
        cur.executemany(insert_query, entries)
        conn.commit()
    print(f"[DEBUG] Zapisano {len(entries)} wpisów do bazy.")

def create_dataframe():
    with connect_db() as conn:
        df = pd.read_sql("SELECT * FROM lockpicking_stats", conn)
    if df.empty:
        print("[DEBUG] Brak danych do wyświetlenia.")
        return None

    grouped = (
        df.groupby(['nick', 'lock_type'])
        .agg(
            Wszystkie=('success', 'count'),
            Udane=('success', lambda x: sum(x)),
            Nieudane=('success', lambda x: len(x) - sum(x)),
            Skuteczność=('success', lambda x: f"{(sum(x)/len(x)*100):.1f}%"),
            Średni_czas=('time', lambda x: f"{x.mean():.2f}s")
        )
        .reset_index()
        .sort_values(['nick', 'lock_type'])
    )
    grouped.columns = ['Nick', 'Rodzaj zamka', 'Wszystkie', 'Udane', 'Nieudane', 'Skuteczność', 'Średni czas']
    print("[DEBUG] Utworzono tabelę podsumowującą.")
    return grouped

def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysyłki na webhook.")
        return

    tabela = tabulate(df.values.tolist(), headers=df.columns.tolist(), tablefmt="github", stralign="center", numalign="center")
    payload = {
        "content": f"```\n{tabela}\n```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[DEBUG] Tabela wysłana poprawnie.")
    else:
        print(f"[ERROR] Błąd przy wysyłaniu: {response.status_code} - {response.text}")

# --- Główna procedura ---
def analyze_ftp_logs():
    log_files, ftps = fetch_ftp_log_files()
    all_entries = []

    for filename in log_files:
        content = download_and_decode_log(ftps, filename)
        entries = parse_log_content(content)
        all_entries.extend(entries)

    ftps.quit()
    insert_entries_to_db(all_entries)
    df = create_dataframe()
    send_to_discord(df)

# --- Uruchomienie ---
if __name__ == "__main__":
    print("[DEBUG] Start analizy logów z FTP...")
    analyze_ftp_logs()
    app.run(host='0.0.0.0', port=3000)
