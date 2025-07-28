import ftplib
import io
import re
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from tabulate import tabulate

# Dane FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# Dane DB Neon
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Regex do parsowania linii logu
LOG_PATTERN = re.compile(
    r'User: (?P<nick>[^\s]+) \([0-9]+, [0-9]+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[0-9.]+)\. Failed attempts: (?P<failed>\d+)\. '
    r'Target object: [^(]+\((?:ID: [^)]+)?\)\. Lock type: (?P<lock_type>\w+)\. User owner: [^\.]+\.\ Location: X=[-0-9.]+ Y=[-0-9.]+ Z=[-0-9.]+'
)

def connect_ftp():
    print("[DEBUG] Łączenie z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")
    return ftp

def ftp_list_files(ftp, path):
    # Serwer nie obsługuje nlst, używamy dir + parsowanie
    files = []

    def dir_callback(line):
        # Przykład linii: -rw-r--r--   1 user group       1234 Jul 28 10:20 gameplay_20250728080038.log
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            filename = parts[8]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                files.append(filename)

    ftp.cwd(path)
    ftp.dir(dir_callback)
    print(f"[DEBUG] Znaleziono plików: {len(files)}")
    return sorted(files)

def ftp_download_file(ftp, filepath):
    buffer = io.BytesIO()
    ftp.retrbinary(f"RETR {filepath}", buffer.write)
    buffer.seek(0)
    content = buffer.read().decode("utf-16-le")
    print(f"[INFO] Wczytano plik: {filepath.split('/')[-1]}")
    return content

def parse_log_content(content):
    entries = []
    for line in content.splitlines():
        if "[LockpickingMinigame_C]" in line:
            match = LOG_PATTERN.search(line)
            if match:
                success = match.group("success") == "Yes"
                entries.append({
                    "nick": match.group("nick"),
                    "lock_type": match.group("lock_type"),
                    "attempts": 1 + int(match.group("failed")),  # 1 success/fail + failed attempts
                    "successes": 1 if success else 0,
                    "failures": 0 if success else 1,
                    "total_time": float(match.group("time"))
                })
    return entries

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_stats (
                nick TEXT NOT NULL,
                lock_type TEXT NOT NULL,
                attempts INT NOT NULL,
                successes INT NOT NULL,
                failures INT NOT NULL,
                total_time FLOAT NOT NULL
            );
        """)
        conn.commit()
    print("[DEBUG] Tabela lockpicking_stats sprawdzona/utworzona.")

def save_entries(conn, entries):
    # Zsumuj wpisy po nick + lock_type przed zapisem, by ograniczyć liczbę insertów
    summary = {}
    for e in entries:
        key = (e["nick"], e["lock_type"])
        if key not in summary:
            summary[key] = {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "total_time": 0.0
            }
        summary[key]["attempts"] += e["attempts"]
        summary[key]["successes"] += e["successes"]
        summary[key]["failures"] += e["failures"]
        summary[key]["total_time"] += e["total_time"]

    # Wstaw lub aktualizuj dane w DB - upsert
    with conn.cursor() as cur:
        for (nick, lock_type), data in summary.items():
            cur.execute("""
                INSERT INTO lockpicking_stats (nick, lock_type, attempts, successes, failures, total_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (nick, lock_type) DO UPDATE SET
                    attempts = lockpicking_stats.attempts + EXCLUDED.attempts,
                    successes = lockpicking_stats.successes + EXCLUDED.successes,
                    failures = lockpicking_stats.failures + EXCLUDED.failures,
                    total_time = lockpicking_stats.total_time + EXCLUDED.total_time
                ;
            """, (nick, lock_type, data["attempts"], data["successes"], data["failures"], data["total_time"]))
        conn.commit()

def fetch_and_display_table(conn):
    df = pd.read_sql("SELECT * FROM lockpicking_stats ORDER BY nick, lock_type;", conn)
    if df.empty:
        print("[INFO] Brak danych w tabeli lockpicking_stats.")
        return
    # Dodaj kolumnę skuteczności i średni czas
    df["Skuteczność (%)"] = (df["successes"] / df["attempts"] * 100).round(2)
    df["Średni czas"] = (df["total_time"] / df["attempts"]).round(2)

    # Przygotuj do wyświetlenia
    display_df = df.rename(columns={
        "nick": "Nick",
        "lock_type": "Zamek",
        "attempts": "Ilość wszystkich prób",
        "successes": "Udane",
        "failures": "Nieudane",
        "Skuteczność (%)": "Skuteczność",
        "Średni czas": "Średni czas"
    })[
        ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    ]

    # Wyświetl tabelę wyśrodkowaną w konsoli
    table = tabulate(display_df, headers="keys", tablefmt="fancy_grid", stralign="center", numalign="center")
    print("[INFO] Tabela lockpicking_stats:")
    print(table)

def main():
    try:
        ftp = connect_ftp()
        files = ftp_list_files(ftp, FTP_LOG_DIR)
        if not files:
            print("[ERROR] Brak plików gameplay_*.log na FTP.")
            return

        conn = psycopg2.connect(**DB_CONFIG)
        create_table_if_not_exists(conn)

        # Pobierz i przetwórz wszystkie pliki za każdym razem (bez sprawdzania, czy były już przetwarzane)
        all_entries = []
        for filename in files:
            try:
                filepath = FTP_LOG_DIR + filename
                content = ftp_download_file(ftp, filepath)
                entries = parse_log_content(content)
                print(f"[INFO] Przetwarzam plik: {filename} -> {len(entries)} wpisów")
                all_entries.extend(entries)
            except Exception as e:
                print(f"[ERROR] Błąd przetwarzania pliku {filename}: {e}")

        if all_entries:
            # Przed zapisaniem czyścimy tabelę, żeby nie kumulować wielokrotnie (skoro przetwarzamy wszystkie pliki)
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE lockpicking_stats;")
                conn.commit()
            save_entries(conn, all_entries)
            fetch_and_display_table(conn)
        else:
            print("[INFO] Brak rozpoznanych wpisów w logach.")

        ftp.quit()
        conn.close()
        print("[DEBUG] Zakończono działanie programu.")
    except Exception as e:
        print(f"[ERROR] Wystąpił błąd: {e}")

if __name__ == "__main__":
    main()
