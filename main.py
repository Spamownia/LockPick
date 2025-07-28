import os
import re
import psycopg2
from ftplib import FTP
from io import BytesIO

# --- KONFIGURACJA FTP ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# --- KONFIGURACJA DB ---
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- FOLDER NA POBRANE LOGI ---
LOG_DIR = "downloaded_logs"
os.makedirs(LOG_DIR, exist_ok=True)

# --- PARSER ---
def parse_log_content(content):
    entries = []
    pattern = re.compile(
        r'User: (.+?) \(\d+, \d+\)\. Success: (Yes|No)\. Elapsed time: ([\d.]+).*?Target object: ([\w_]+)',
        re.DOTALL
    )
    for match in pattern.finditer(content):
        nick, wynik, czas_str, zamek = match.groups()
        czas_ms = int(float(czas_str) * 1000)
        entries.append((
            nick.strip(),
            zamek.strip(),
            "SUCCESS" if wynik == "Yes" else "FAIL",
            czas_ms
        ))
    return entries

# --- ZAPIS DO DB ---
def save_to_database(entries):
    if not entries:
        print("[INFO] Brak wpisów do zapisania.")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_attempts (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            zamek TEXT,
            wynik TEXT,
            czas_ms INTEGER
        );
    """)
    for entry in entries:
        cur.execute("""
            INSERT INTO lockpicking_attempts (nick, zamek, wynik, czas_ms)
            VALUES (%s, %s, %s, %s);
        """, entry)
    conn.commit()
    cur.close()
    conn.close()
    print(f"[INFO] Zapisano {len(entries)} wpisów do bazy danych.")

# --- POBIERZ LOGI ---
def fetch_and_parse_logs():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=15)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")

    filenames = []
    ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))

    log_files = [f for f in filenames if f.startswith("gameplay") and f.endswith(".log")]
    print(f"[INFO] Znaleziono {len(log_files)} plików gameplay_*.log")

    total_entries = []

    for filename in log_files:
        print(f"[INFO] Przetwarzanie: {filename}")
        bio = BytesIO()
        try:
            ftp.retrbinary(f"RETR {filename}", bio.write)
        except Exception as e:
            print(f"[BŁĄD] Nie udało się pobrać {filename}: {e}")
            continue

        content = bio.getvalue().decode("utf-16-le", errors="ignore")
        local_path = os.path.join(LOG_DIR, filename)
        with open(local_path, "w", encoding="utf-16-le") as f:
            f.write(content)

        parsed = parse_log_content(content)
        print(f"[INFO] {len(parsed)} wpisów w pliku {filename}")
        total_entries.extend(parsed)

    ftp.quit()
    return total_entries

# --- MAIN ---
if __name__ == "__main__":
    print("[DEBUG] Start programu")
    entries = fetch_and_parse_logs()
    save_to_database(entries)
    print("[DEBUG] Zakończono")
