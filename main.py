import os
import re
import ftplib
import psycopg2
import ssl

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

def parse_float_safely(s):
    s = s.strip().replace(',', '.').rstrip('.')
    try:
        return float(s)
    except ValueError:
        return None

def parse_log_content(content):
    pattern = re.compile(
        r'User:\s+(?P<nick>\w+).*?Success:\s+(?P<success>\w+).*?Elapsed time:\s+(?P<czas>[\d.,]+).*?Failed attempts:\s+(?P<nieudane>\d+).*?Lock type:\s+(?P<zamek>\w+)',
        re.DOTALL
    )
    entries = []
    lines = content.splitlines()
    for line in lines:
        if "User:" in line and "Success:" in line:
            match = pattern.search(line)
            if match:
                nick = match.group("nick")
                success = match.group("success").strip().lower() in ["yes", "true", "1"]
                czas_raw = match.group("czas")
                czas_float = parse_float_safely(czas_raw)
                if czas_float is None:
                    print(f"[WARNING] Nieprawidłowy czas: '{czas_raw}' → linia: {line}")
                    continue
                czas_ms = int(czas_float * 1000)
                nieudane = int(match.group("nieudane"))
                zamek = match.group("zamek")

                entries.append({
                    "nick": nick,
                    "zamek": zamek,
                    "sukces": success,
                    "czas_ms": czas_ms,
                    "nieudane": nieudane
                })
            else:
                print(f"[DEBUG] Pominięto niedopasowaną linię: {line}")
    return entries

def save_to_database(entries):
    try:
        context = ssl.create_default_context()
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_logs (
                id SERIAL PRIMARY KEY,
                nick TEXT,
                zamek TEXT,
                sukces BOOLEAN,
                czas_ms INTEGER,
                nieudane INTEGER
            )
        """)
        for e in entries:
            cur.execute("""
                INSERT INTO lockpicking_logs (nick, zamek, sukces, czas_ms, nieudane)
                VALUES (%s, %s, %s, %s, %s)
            """, (e["nick"], e["zamek"], e["sukces"], e["czas_ms"], e["nieudane"]))
        conn.commit()
        cur.close()
        conn.close()
        print(f"[DB] Zapisano {len(entries)} rekordów do bazy")
    except Exception as e:
        print(f"[DB ERROR] {e}")

def fetch_and_parse_logs():
    print("[DEBUG] Start programu")
    entries = []
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")
        files = []
        ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
        target_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
        print(f"[INFO] Znaleziono {len(target_files)} plików gameplay_*.log")
        for fname in target_files:
            print(f"[INFO] Przetwarzanie: {fname}")
            try:
                from io import BytesIO
                buffer = BytesIO()
                ftp.retrbinary(f"RETR {fname}", buffer.write)
                content = buffer.getvalue().decode("utf-16le", errors="ignore")
                parsed = parse_log_content(content)
                print(f"[INFO] Wpisy w {fname}: {len(parsed)}")
                entries.extend(parsed)
            except Exception as e:
                print(f"[ERROR] Błąd przy pliku {fname}: {e}")
    return entries

if __name__ == "__main__":
    entries = fetch_and_parse_logs()
    if entries:
        save_to_database(entries)
    else:
        print("[INFO] Brak danych do zapisania.")
