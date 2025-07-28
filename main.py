import time
import re
import io
from ftplib import FTP, error_perm
import psycopg2
import psycopg2.extras
from tabulate import tabulate

# Konfiguracja FTP i bazy danych
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

LOG_ENTRY_REGEX = re.compile(
    r"User: (?P<nick>\S+) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. "
    r"Elapsed time: (?P<elapsed>\d+\.\d+)\. Failed attempts: \d+\. "
    r"Target object: .+?\. Lock type: (?P<lock_type>\S+)\."
)

# Tworzenie tabeli w bazie
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_stats (
            id SERIAL PRIMARY KEY,
            nickname TEXT NOT NULL,
            lock_type TEXT NOT NULL,
            success BOOLEAN NOT NULL,
            elapsed_time FLOAT NOT NULL,
            log_file TEXT NOT NULL,
            processed_at TIMESTAMP DEFAULT NOW()
        );
        """)
        conn.commit()
    print("[DEBUG] Tabela lockpicking_stats sprawdzona/utworzona.")

# Wyczyść tabelę przed nowym pełnym przetworzeniem
def clear_table(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE lockpicking_stats;")
        conn.commit()
    print("[DEBUG] Tabela lockpicking_stats wyczyszczona.")

# Pobranie listy plików gameplay_*.log z FTP bez NLST (obsługa błędu)
def ftp_list_files(ftp, path):
    files = []
    try:
        entries = list(ftp.mlsd(path))
        for name, facts in entries:
            if name.startswith("gameplay_") and name.endswith(".log"):
                files.append(name)
    except (error_perm, AttributeError):
        lines = []
        ftp.retrlines(f"LIST {path}", lines.append)
        for line in lines:
            parts = line.split()
            if len(parts) >= 9:
                fname = parts[-1]
                if fname.startswith("gameplay_") and fname.endswith(".log"):
                    files.append(fname)
    return files

# Pobranie pliku i odczyt z kodowaniem UTF-16 LE
def ftp_download_file(ftp, filepath):
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {filepath}", bio.write)
    bio.seek(0)
    content = bio.read().decode("utf-16-le")
    return content

# Parsowanie zawartości pliku logu - zwraca listę słowników
def parse_log_content(content):
    entries = []
    for line in content.splitlines():
        m = LOG_ENTRY_REGEX.search(line)
        if m:
            entries.append({
                "nickname": m.group("nick"),
                "lock_type": m.group("lock_type"),
                "success": m.group("success") == "Yes",
                "elapsed_time": float(m.group("elapsed"))
            })
    return entries

# Zapis do bazy danych
def save_entries(conn, entries, log_file):
    with conn.cursor() as cur:
        for e in entries:
            cur.execute("""
                INSERT INTO lockpicking_stats (nickname, lock_type, success, elapsed_time, log_file)
                VALUES (%s, %s, %s, %s, %s)
            """, (e["nickname"], e["lock_type"], e["success"], e["elapsed_time"], log_file))
        conn.commit()

# Pobranie agregowanych danych z bazy do tabeli
def fetch_aggregated_stats(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
        SELECT
            nickname,
            lock_type,
            COUNT(*) AS attempts,
            SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS fail_count,
            ROUND(100.0 * SUM(CASE WHEN success THEN 1 ELSE 0 END) / COUNT(*), 2) AS efficiency,
            ROUND(AVG(elapsed_time), 2) AS avg_elapsed_time
        FROM lockpicking_stats
        GROUP BY nickname, lock_type
        ORDER BY nickname, lock_type
        """)
        return cur.fetchall()

# Wyświetlanie tabeli w konsoli
def display_stats_table(rows):
    if not rows:
        print("[INFO] Brak danych do wyświetlenia.")
        return
    headers = ["Nick", "Lock Type", "Attempts", "Success", "Fail", "Efficiency [%]", "Avg Time [s]"]
    table = []
    for r in rows:
        table.append([
            r["nickname"],
            r["lock_type"],
            r["attempts"],
            r["success_count"],
            r["fail_count"],
            r["efficiency"],
            r["avg_elapsed_time"]
        ])
    print(tabulate(table, headers=headers, tablefmt="grid", stralign="center"))

# Główna funkcja programu
def main():
    print("[DEBUG] Start programu")
    # Połączenie z FTP
    ftp = FTP()
    try:
        ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")
        ftp.cwd(FTP_LOG_DIR)
    except Exception as e:
        print(f"[ERROR] Błąd FTP: {e}")
        return

    # Połączenie z bazą danych
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        create_table(conn)
        clear_table(conn)  # <-- tu wyczyszczenie tabeli
    except Exception as e:
        print(f"[ERROR] Błąd bazy danych: {e}")
        return

    # Pobranie listy plików
    try:
        files = ftp_list_files(ftp, FTP_LOG_DIR)
        print(f"[DEBUG] Znaleziono plików: {len(files)}")
    except Exception as e:
        print(f"[ERROR] Błąd pobierania listy plików FTP: {e}")
        return

    for filename in sorted(files):
        try:
            full_path = FTP_LOG_DIR + filename
            content = ftp_download_file(ftp, full_path)
            entries = parse_log_content(content)
            print(f"[INFO] Przetwarzam plik: {filename}")
            print(f"[DEBUG] {filename} -> {len(entries)} wpisów")
            if entries:
                save_entries(conn, entries, filename)
            else:
                print(f"[INFO] Plik {filename} nie zawiera rozpoznanych wpisów.")
        except Exception as e:
            print(f"[ERROR] Błąd przetwarzania pliku {filename}: {e}")

    # Agregacja i wyświetlenie statystyk
    rows = fetch_aggregated_stats(conn)
    display_stats_table(rows)

    # Zamknięcie połączeń
    try:
        ftp.quit()
    except:
        pass
    conn.close()
    print("[DEBUG] Koniec programu")

if __name__ == "__main__":
    main()
