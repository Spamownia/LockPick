import os
import re
import psycopg2
from ftplib import FTP
from io import BytesIO
import requests

# -----------------------------------
# KONFIGURACJA
# -----------------------------------
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

# -----------------------------------
# PARSER LOGÓW
# -----------------------------------
def parse_log_content(content):
    entries = []
    lines = content.splitlines()
    for line in lines:
        if "[LogMinigame]" in line and "LockpickingMinigame_C" in line:
            try:
                nick_match = re.search(r"User:\s*(.*?)\s+\(", line)
                success_match = re.search(r"Success:\s*(Yes|No)", line)
                czas_match = re.search(r"Elapsed time:\s*([\d.]+)", line)
                failed_match = re.search(r"Failed attempts:\s*(\d+)", line)
                locktype_match = re.search(r"Lock type:\s*(\w+)", line)

                if not all([nick_match, success_match, czas_match, failed_match, locktype_match]):
                    continue

                nick = nick_match.group(1).strip()
                success = success_match.group(1) == "Yes"
                czas_str = czas_match.group(1).rstrip(".")
                czas_ms = int(float(czas_str) * 1000)
                failed = int(failed_match.group(1))
                locktype = locktype_match.group(1)

                entries.append({
                    "nick": nick,
                    "success": success,
                    "failed": failed,
                    "czas_ms": czas_ms,
                    "zamek": locktype
                })
            except Exception as e:
                print(f"[WARN] Błąd parsowania: {e}")
                print(f"[WARN] Linia: {line}")
    print(f"[DEBUG] Sparsowano {len(entries)} wpisów z logu")
    return entries

# -----------------------------------
# POŁĄCZENIE Z FTP I POBRANIE LOGÓW
# -----------------------------------
def fetch_and_parse_logs():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)

    files = []
    ftp.retrlines("LIST", lambda x: files.append(x.split()[-1]))

    log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[INFO] Znaleziono {len(log_files)} plików gameplay_*.log")

    all_entries = []
    for filename in log_files:
        print(f"[INFO] Przetwarzanie: {filename}")
        buffer = BytesIO()
        try:
            ftp.retrbinary(f"RETR {filename}", buffer.write)
        except Exception as e:
            print(f"[ERROR] Nie można pobrać pliku {filename}: {e}")
            continue
        content = buffer.getvalue().decode("utf-16-le", errors="ignore")
        parsed = parse_log_content(content)
        all_entries.extend(parsed)

    ftp.quit()
    return all_entries

# -----------------------------------
# ZAPIS DO BAZY DANYCH
# -----------------------------------
def save_to_database(entries):
    if not entries:
        print("[INFO] Brak danych do zapisania.")
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_log (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            success BOOLEAN,
            failed INT,
            czas_ms INT,
            zamek TEXT
        );
    """)
    for e in entries:
        cur.execute("""
            INSERT INTO lockpicking_log (nick, success, failed, czas_ms, zamek)
            VALUES (%s, %s, %s, %s, %s);
        """, (e["nick"], e["success"], e["failed"], e["czas_ms"], e["zamek"]))
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DB] Zapisano {len(entries)} rekordów do bazy")

# -----------------------------------
# GŁÓWNE WYWOŁANIE
# -----------------------------------
if __name__ == "__main__":
    print("[DEBUG] Start programu")
    try:
        entries = fetch_and_parse_logs()
        save_to_database(entries)
    except Exception as e:
        print(f"[FATAL] Błąd główny: {e}")
