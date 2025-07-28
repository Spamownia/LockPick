import ftplib
import io
import psycopg2
import pandas as pd
import requests
import re
from tabulate import tabulate

# Konfiguracja
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Funkcja parsująca zawartość loga
def parse_log_content(content):
    pattern = re.compile(
        r"User:\s*(?P<nick>.+?)\s+.*?Success:\s*(?P<success>Yes|No)\.\s+Elapsed time:\s*(?P<elapsed>[\d.]+)s.*?Lock type:\s*(?P<lock>.+?)\.",
        re.DOTALL
    )
    entries = []
    for match in pattern.finditer(content):
        success = True if match.group("success") == "Yes" else False
        entries.append({
            "nick": match.group("nick").strip(),
            "lock_type": match.group("lock").strip(),
            "success": success,
            "elapsed_time": float(match.group("elapsed")),
        })
    return entries

# Pobranie listy plików z FTP (z pominięciem nlst)
def fetch_log_files(ftp):
    files = []
    print("[DEBUG] Pobieranie listy plików przez ftp.dir()...")
    lines = []
    ftp.dir(FTP_LOG_PATH, lines.append)
    for line in lines:
        parts = line.split()
        if len(parts) < 9:
            continue
        filename = parts[-1]
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            files.append(filename)
    print(f"[DEBUG] Znaleziono plików: {len(files)}")
    return files

# Pobranie i dekodowanie pliku
def download_file(ftp, filename):
    buffer = io.BytesIO()
    ftp.retrbinary(f"RETR {FTP_LOG_PATH}{filename}", buffer.write)
    buffer.seek(0)
    content = buffer.read().decode("utf-16le")
    return content

# Tworzenie tabeli w bazie jeśli nie istnieje
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_stats (
                id SERIAL PRIMARY KEY,
                nick TEXT NOT NULL,
                lock_type TEXT NOT NULL,
                success BOOLEAN NOT NULL,
                elapsed_time REAL NOT NULL,
                log_file TEXT NOT NULL
            )
        """)
        conn.commit()
    print("[DEBUG] Tabela lockpicking_stats sprawdzona/utworzona.")

# Zapis wpisów do bazy (wszystkich, bez filtrowania)
def insert_entries(conn, entries, log_file):
    if not entries:
        return
    with conn.cursor() as cur:
        for e in entries:
            cur.execute("""
                INSERT INTO lockpicking_stats (nick, lock_type, success, elapsed_time, log_file)
                VALUES (%s, %s, %s, %s, %s)
            """, (e["nick"], e["lock_type"], e["success"], e["elapsed_time"], log_file))
        conn.commit()
    print(f"[INFO] Zapisano {len(entries)} wpisów z pliku {log_file} do bazy.")

# Pobranie wszystkich wpisów z bazy do Pandas DataFrame
def fetch_all_entries(conn):
    df = pd.read_sql("SELECT nick, lock_type, success, elapsed_time FROM lockpicking_stats", conn)
    return df

# Agregacja danych i przygotowanie tabeli do wysłania
def prepare_summary_table(df):
    if df.empty:
        return None

    grouped = df.groupby(['nick', 'lock_type']).agg(
        attempts=pd.NamedAgg(column='success', aggfunc='count'),
        successes=pd.NamedAgg(column='success', aggfunc='sum'),
        failures=pd.NamedAgg(column='success', aggfunc=lambda x: (~x).sum()),
        avg_time=pd.NamedAgg(column='elapsed_time', aggfunc='mean')
    ).reset_index()

    grouped['efficiency'] = (grouped['successes'] / grouped['attempts'] * 100).round(2)
    grouped['avg_time'] = grouped['avg_time'].round(2)

    grouped.rename(columns={
        'nick': 'Nick',
        'lock_type': 'Zamek',
        'attempts': 'Ilość wszystkich prób',
        'successes': 'Udane',
        'failures': 'Nieudane',
        'efficiency': 'Skuteczność [%]',
        'avg_time': 'Średni czas [s]'
    }, inplace=True)

    # Przygotowanie tabeli w formacie tekstowym z wyśrodkowaniem i dopasowaniem kolumn
    table = tabulate(grouped, headers='keys', tablefmt='pipe', stralign='center', numalign='center')
    return table

# Wysyłka tabeli do Discord webhook
def send_to_webhook(message):
    data = {"content": f"```\n{message}\n```"}
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code == 204:
        print("[INFO] Tabela wysłana na webhook Discord.")
    else:
        print(f"[ERROR] Błąd wysyłki webhook: {response.status_code} {response.text}")

def main():
    print("[DEBUG] Start programu")

    # Połączenie do FTP
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")

    # Pobierz listę plików
    log_files = fetch_log_files(ftp)

    # Połączenie z bazą
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        sslmode=DB_CONFIG["sslmode"]
    )

    create_table(conn)

    total_entries = 0

    # Pobierz i przetwórz pliki
    for filename in log_files:
        content = download_file(ftp, filename)
        print(f"[INFO] Załadowano: {filename}")
        entries = parse_log_content(content)
        print(f"[INFO] Przetwarzam plik: {filename} -> {len(entries)} wpisów")
        insert_entries(conn, entries, filename)
        total_entries += len(entries)

    ftp.quit()
    print(f"[DEBUG] Wszystkich wpisów: {total_entries}")

    # Pobierz wszystkie wpisy z bazy i przygotuj tabelę
    df = fetch_all_entries(conn)

    if df.empty:
        print("[INFO] Brak danych do wysłania.")
        conn.close()
        return

    table_text = prepare_summary_table(df)
    if table_text:
        send_to_webhook(table_text)

    conn.close()

if __name__ == "__main__":
    main()
