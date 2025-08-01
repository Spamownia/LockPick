import os
import re
import io
import time
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from ftplib import FTP
from flask import Flask

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- Konfiguracja ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            lock_type TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            log_file TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DEBUG] Baza danych gotowa.")

def parse_log_content(content, log_filename):
    # Dekodowanie UTF-16 LE, ignorowanie błędów
    content = content.decode('utf-16-le', errors='ignore')
    entries = []

    # Regex zgodny z faktycznym wzorcem logów (uwzględnia spacje, kropki, nawiasy, itd.)
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>\w+) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<elapsed_time>\d+\.\d+)\.",
        re.MULTILINE
    )

    # Dodatkowo wyszukiwanie lock_type w dalszej części tej samej linii
    lock_type_pattern = re.compile(r"Lock type: (?P<lock_type>\w+)\.")

    for line in content.splitlines():
        match = pattern.search(line)
        if match:
            nick = match.group("nick")
            success = match.group("success") == "Yes"
            elapsed_time = float(match.group("elapsed_time"))

            lock_type_match = lock_type_pattern.search(line)
            lock_type = lock_type_match.group("lock_type") if lock_type_match else "Unknown"

            entries.append((nick, lock_type, success, elapsed_time, log_filename))

    print(f"[DEBUG] Przetworzono {len(entries)} wpisów z pliku {log_filename}")
    return entries

def fetch_ftp_log_files():
    print("[DEBUG] Łączenie z FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)

    log_files = []
    ftp.retrlines('LIST', lambda line: log_files.append(line.split()[-1]))

    gameplay_logs = [name for name in log_files if name.startswith("gameplay_") and name.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(gameplay_logs)} plików gameplay_*.log")

    return gameplay_logs, ftp

def fetch_and_process_logs_from_ftp():
    gameplay_logs, ftp = fetch_ftp_log_files()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    new_entries = []

    for log_file in gameplay_logs:
        cur.execute("SELECT 1 FROM lockpicking WHERE log_file = %s LIMIT 1", (log_file,))
        if cur.fetchone():
            print(f"[DEBUG] Plik {log_file} już przetworzony, pomijam.")
            continue

        print(f"[DEBUG] Pobieranie pliku: {log_file}")
        buffer = io.BytesIO()
        ftp.retrbinary(f"RETR {log_file}", buffer.write)
        buffer.seek(0)
        entries = parse_log_content(buffer.read(), log_file)
        new_entries.extend(entries)

        for entry in entries:
            cur.execute("""
                INSERT INTO lockpicking (nick, lock_type, success, elapsed_time, log_file)
                VALUES (%s, %s, %s, %s, %s)
            """, entry)

    conn.commit()
    cur.close()
    conn.close()
    ftp.quit()

    print(f"[DEBUG] Zapisano {len(new_entries)} nowych wpisów do bazy.")
    return new_entries

def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query("SELECT * FROM lockpicking", conn)
    conn.close()

    if df.empty:
        print("[DEBUG] Brak danych do wyświetlenia.")
        return None

    df["success_int"] = df["success"].astype(int)
    df["failed"] = 1 - df["success_int"]

    grouped = df.groupby(["nick", "lock_type"]).agg(
        Próby=pd.NamedAgg(column="success", aggfunc="count"),
        Udane=pd.NamedAgg(column="success_int", aggfunc="sum"),
        Nieudane=pd.NamedAgg(column="failed", aggfunc="sum"),
        Skuteczność=pd.NamedAgg(column="success_int", aggfunc=lambda x: round(100 * x.sum() / len(x), 1)),
        Średni_czas=pd.NamedAgg(column="elapsed_time", aggfunc="mean")
    ).reset_index()

    grouped["Średni_czas"] = grouped["Średni_czas"].round(2)

    # Sortowanie po nicku i lock_type
    grouped = grouped.sort_values(by=["nick", "lock_type"]).reset_index(drop=True)

    return grouped

def format_table_for_webhook(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return ""

    # Nazwy kolumn po polsku i w kolejności
    columns = ["nick", "lock_type", "Próby", "Udane", "Nieudane", "Skuteczność", "Średni_czas"]
    # Mapowanie kolumn na nagłówki
    headers = ["Nick", "Rodzaj zamka", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    # Przygotowanie tabeli z wyśrodkowaniem
    # Obliczamy szerokość kolumn wg najdłuższego tekstu
    col_widths = []
    for col in columns:
        max_len = max(df[col].astype(str).map(len).max(), len(headers[columns.index(col)]))
        col_widths.append(max_len)

    # Formatowanie nagłówka
    header_row = " | ".join(f"{headers[i]:^{col_widths[i]}}" for i in range(len(headers)))
    separator_row = "-|-".join("-" * col_widths[i] for i in range(len(headers)))

    # Formatowanie wierszy danych
    data_rows = []
    for _, row in df.iterrows():
        row_str = " | ".join(f"{str(row[col]):^{col_widths[i]}}" for i, col in enumerate(columns))
        data_rows.append(row_str)

    table = "\n".join([header_row, separator_row] + data_rows)
    return table

def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Nie znaleziono danych do wysłania.")
        return

    table_text = format_table_for_webhook(df)
    print("[DEBUG] Tabela do wysłania:\n", table_text)

    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table_text}\n```"})
    if response.status_code == 204:
        print("[DEBUG] Wysłano dane na Discord.")
    else:
        print(f"[DEBUG] Błąd wysyłania na Discord: {response.status_code}, {response.text}")

if __name__ == "__main__":
    print("[DEBUG] Start procesu FTP i analizy logów")
    init_db()
    new_data = fetch_and_process_logs_from_ftp()
    if new_data:
        df = create_dataframe()
        send_to_discord(df)
    else:
        print("[DEBUG] Brak nowych danych do przetworzenia.")
    app.run(host="0.0.0.0", port=3000)
