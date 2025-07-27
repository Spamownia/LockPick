import os
import ftplib
import psycopg2
import re
from datetime import datetime

# FTP KONFIGURACJA
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# BAZA KONFIGURACJA (Neon)
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Katalog lokalny do zapisu logów
LOCAL_DIR = "downloaded_logs"

def debug(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [DEBUG] {msg}")

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    debug(f"Połączono z FTP: {FTP_HOST}:{FTP_PORT}")
    return ftp

def list_log_files(ftp):
    ftp.cwd(FTP_DIR)
    debug(f"Zmieniono katalog: {FTP_DIR}")
    files = []
    ftp.retrlines('LIST', lambda line: files.append(line))
    gameplay_files = [line.split()[-1] for line in files if line.split()[-1].startswith("gameplay") and line.endswith(".log")]
    debug(f"Znaleziono {len(gameplay_files)} plików gameplay_*.log")
    return gameplay_files

def download_files(ftp, filenames):
    os.makedirs(LOCAL_DIR, exist_ok=True)
    for filename in filenames:
        local_path = os.path.join(LOCAL_DIR, filename)
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR " + filename, f.write)
        debug(f"Pobrano: {filename}")

def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    debug("Połączono z bazą danych PostgreSQL")
    return conn

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicks (
                id SERIAL PRIMARY KEY,
                nick TEXT,
                zamek TEXT,
                wynik TEXT,
                czas_ms INTEGER,
                data TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        debug("Tabela 'lockpicks' gotowa.")

def parse_log_content(content):
    entries = []
    pattern = re.compile(r'\[(.*?)\] \[LockPicking\] Player (.+?) tried to pick (.+?): (SUCCESS|FAIL) in (\d+)ms')
    for match in pattern.finditer(content):
        _, nick, zamek, wynik, czas_ms = match.groups()
        entries.append((nick.strip(), zamek.strip(), wynik, int(czas_ms)))
    return entries

def process_logs_and_save_to_db(conn):
    total = 0
    for filename in os.listdir(LOCAL_DIR):
        if not filename.startswith("gameplay") or not filename.endswith(".log"):
            continue

        path = os.path.join(LOCAL_DIR, filename)
        try:
            with open(path, "r", encoding="utf-16-le") as f:
                content = f.read()
        except Exception as e:
            debug(f"Błąd odczytu pliku {filename}: {e}")
            continue

        entries = parse_log_content(content)
        if not entries:
            debug(f"Brak wpisów w {filename}")
            continue

        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO lockpicks (nick, zamek, wynik, czas_ms)
                VALUES (%s, %s, %s, %s);
            """, entries)
            conn.commit()
            debug(f"Zapisano {len(entries)} wpisów z {filename} do bazy danych")
            total += len(entries)

    debug(f"Zapisano łącznie {total} wpisów do bazy danych.")

def main():
    debug("Start programu")
    try:
        ftp = connect_ftp()
        files = list_log_files(ftp)
        download_files(ftp, files)
        ftp.quit()
        debug("Rozłączono z FTP")
    except Exception as e:
        debug(f"[BŁĄD FTP] {e}")
        return

    try:
        conn = connect_db()
        init_db(conn)
        process_logs_and_save_to_db(conn)
        conn.close()
        debug("Rozłączono z bazą danych")
    except Exception as e:
        debug(f"[BŁĄD DB] {e}")

if __name__ == "__main__":
    main()
