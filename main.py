import re
import ftplib
import io
import time
import psycopg2
import pandas as pd
import requests
from tabulate import tabulate

# --- Konfiguracja ---
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
    "sslmode": "require",
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Funkcje ---

def list_files_ftp(ftp, path):
    """Lista plików w katalogu FTP bez użycia nlst() (serwer nie obsługuje)."""
    print("[DEBUG] Pobieram listę plików z FTP...")
    file_list = []
    lines = []
    def collect_lines(line):
        lines.append(line)
    ftp.cwd(path)
    ftp.retrlines("LIST", collect_lines)
    # Parsowanie listingu w stylu UNIX, wyciągnięcie nazwy pliku (ostatni token)
    for line in lines:
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            filename = parts[8]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                file_list.append(filename)
    print(f"[DEBUG] Znaleziono plików: {len(file_list)}")
    return file_list

def download_file_ftp(ftp, filename):
    """Pobiera zawartość pliku z FTP, kodowanie UTF-16 LE."""
    print(f"[INFO] Pobieram plik: {filename}")
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    bio.seek(0)
    content = bio.read().decode("utf-16le")
    return content

def parse_log_content(content):
    """Parsuje zawartość loga wg wzoru z przykładu użytkownika."""
    pattern = re.compile(
        r"User:\s*(\w+)\s*\([^\)]+\)\.\s*"
        r"Success:\s*(Yes|No)\.\s*"
        r"Elapsed time:\s*([\d.]+)\.\s*"
        r"Failed attempts:\s*(\d+)\.\s*"
        r".*Lock type:\s*([\w]+)\.",
        re.MULTILINE
    )
    entries = []
    for m in pattern.finditer(content):
        nick = m.group(1)
        success = m.group(2) == "Yes"
        elapsed_time = float(m.group(3))
        failed_attempts = int(m.group(4))
        lock_type = m.group(5)
        entries.append({
            "nick": nick,
            "success": success,
            "elapsed_time": elapsed_time,
            "failed_attempts": failed_attempts,
            "lock_type": lock_type
        })
    print(f"[DEBUG] Znaleziono wpisów: {len(entries)}")
    return entries

def init_db():
    """Inicjalizuje połączenie z bazą i tworzy tabelę jeśli nie istnieje."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lockpicking_stats (
        id SERIAL PRIMARY KEY,
        nick TEXT NOT NULL,
        lock_type TEXT NOT NULL,
        attempts_total INTEGER NOT NULL DEFAULT 0,
        success_count INTEGER NOT NULL DEFAULT 0,
        fail_count INTEGER NOT NULL DEFAULT 0,
        elapsed_sum FLOAT NOT NULL DEFAULT 0.0,
        UNIQUE(nick, lock_type)
    );
    """)
    conn.commit()
    cur.close()
    return conn

def update_db(conn, entries):
    """Aktualizuje bazę danymi z listy wpisów (sumowanie statystyk)."""
    if not entries:
        print("[INFO] Brak nowych wpisów do zapisania w bazie.")
        return 0
    cur = conn.cursor()
    updated_rows = 0
    for e in entries:
        nick = e["nick"]
        lock_type = e["lock_type"]
        attempts = e["failed_attempts"] + (1 if e["success"] else 0)
        success_inc = 1 if e["success"] else 0
        fail_inc = 0 if e["success"] else 1
        elapsed = e["elapsed_time"]
        # Upsert - jeśli wpis istnieje to aktualizuj, w przeciwnym razie dodaj
        cur.execute("""
            INSERT INTO lockpicking_stats (nick, lock_type, attempts_total, success_count, fail_count, elapsed_sum)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (nick, lock_type) DO UPDATE SET
                attempts_total = lockpicking_stats.attempts_total + EXCLUDED.attempts_total,
                success_count = lockpicking_stats.success_count + EXCLUDED.success_count,
                fail_count = lockpicking_stats.fail_count + EXCLUDED.fail_count,
                elapsed_sum = lockpicking_stats.elapsed_sum + EXCLUDED.elapsed_sum
        """, (nick, lock_type, attempts, success_inc, fail_inc, elapsed))
        updated_rows += 1
    conn.commit()
    cur.close()
    print(f"[DEBUG] Zaktualizowano/ dodano {updated_rows} rekordów w bazie.")
    return updated_rows

def fetch_db_stats(conn):
    """Pobiera z bazy dane zgrupowane per nick i lock_type."""
    df = pd.read_sql(
        """
        SELECT nick, lock_type,
               attempts_total,
               success_count,
               fail_count,
               elapsed_sum
        FROM lockpicking_stats
        ORDER BY nick, lock_type
        """,
        conn
    )
    return df

def create_summary_table(df):
    """Tworzy tabelę tekstową z podsumowaniem statystyk."""
    if df.empty:
        return None
    df = df.copy()
    # Skuteczność i średni czas
    df["success_rate"] = df["success_count"] / df["attempts_total"]
    df["avg_elapsed"] = df["elapsed_sum"] / df["attempts_total"]
    df["success_rate"] = df["success_rate"].apply(lambda x: f"{x:.2%}")
    df["avg_elapsed"] = df["avg_elapsed"].apply(lambda x: f"{x:.2f}")

    table = df[["nick", "lock_type", "attempts_total", "success_count", "fail_count", "success_rate", "avg_elapsed"]]
    table.columns = ["Nick", "Lock", "All tries", "Success", "Fail", "Success Rate", "Avg time"]

    # Formatowanie wyśrodkowania i szerokości kolumn
    return tabulate(table, headers="keys", tablefmt="github", stralign="center")

def send_webhook_message(message):
    """Wysyła wiadomość na Discord webhook."""
    if not message:
        print("[INFO] Pusta wiadomość - nie wysyłam.")
        return
    payload = {"content": f"```\n{message}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[INFO] Wiadomość wysłana na webhook.")
    else:
        print(f"[ERROR] Błąd wysyłki webhook: {response.status_code} {response.text}")

def main():
    print("[DEBUG] Start programu")

    # Połączenie z FTP
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")

    # Pobierz listę plików logów
    files = list_files_ftp(ftp, FTP_LOG_PATH)

    # Pobierz zawartość i parsuj
    all_entries = []
    for f in files:
        content = download_file_ftp(ftp, f)
        entries = parse_log_content(content)
        all_entries.extend(entries)

    ftp.quit()

    # Inicjalizacja bazy
    conn = init_db()

    # Aktualizacja bazy
    updated_count = update_db(conn, all_entries)

    if updated_count == 0:
        print("[INFO] Brak danych do wysłania.")
        conn.close()
        return

    # Pobierz podsumowanie i wyślij na webhook
    df_stats = fetch_db_stats(conn)
    conn.close()

    table_text = create_summary_table(df_stats)
    if table_text:
        send_webhook_message(table_text)
    else:
        print("[INFO] Brak danych do wysłania (tabela pusta).")

if __name__ == "__main__":
    main()
