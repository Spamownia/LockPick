import ftplib
import io
import re
import pandas as pd
import psycopg2
from tabulate import tabulate
import requests

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- REGEX DOPASOWANIA ---
LOG_PATTERN = re.compile(
    r"User: (?P<nick>.+?) \(\d+, \d+\)\. Success: (?P<success>\w+)\. Elapsed time: (?P<elapsed>[\d.]+)\. "
    r"Failed attempts: (?P<failed>\d+)\..*?Lock type: (?P<lock_type>\w+)"
)

# --- FUNKCJE ---

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_stats (
                id SERIAL PRIMARY KEY,
                nick TEXT,
                lock_type TEXT,
                success BOOLEAN,
                elapsed FLOAT,
                log_file TEXT
            )
        """)
        conn.commit()

def fetch_log_files():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    filenames = []
    ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]) if "gameplay_" in line else None)
    return ftp, [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]

def download_and_decode_log(ftp, filename):
    buffer = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)
    return buffer.read().decode("utf-16")

def parse_log_content(content, filename):
    entries = []
    for match in LOG_PATTERN.finditer(content):
        nick = match.group("nick").strip()
        success = match.group("success").lower() == "yes"
        elapsed = float(match.group("elapsed"))
        lock_type = match.group("lock_type")
        entries.append((nick, lock_type, success, elapsed, filename))
    return entries

def insert_entries(entries):
    with connect_db() as conn, conn.cursor() as cur:
        for entry in entries:
            cur.execute("""
                INSERT INTO lockpicking_stats (nick, lock_type, success, elapsed, log_file)
                VALUES (%s, %s, %s, %s, %s)
            """, entry)
        conn.commit()

def fetch_all_data():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT nick, lock_type, success, elapsed FROM lockpicking_stats")
        return cur.fetchall()

def build_stats(data):
    df = pd.DataFrame(data, columns=["Nick", "Zamek", "Sukces", "Czas"])
    if df.empty:
        return None
    stats = (
        df.groupby(["Nick", "Zamek"])
        .agg(
            Wszystkie=("Sukces", "count"),
            Udane=("Sukces", "sum"),
            Nieudane=("Sukces", lambda x: (~x).sum()),
            Skuteczność=("Sukces", lambda x: f"{(x.sum() / len(x)) * 100:.1f}%"),
            Średni_czas=("Czas", lambda x: f"{x.mean():.2f}s")
        )
        .reset_index()
    )
    return stats

def format_table(stats_df):
    headers = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    return "```\n" + tabulate(stats_df.values, headers=headers, tablefmt="grid", stralign="center") + "\n```"

def send_to_webhook(message):
    payload = {"content": message}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[WEBHOOK] Status: {response.status_code}")

# --- GŁÓWNA FUNKCJA ---

def main():
    print("[DEBUG] Start programu")
    init_db()
    ftp, files = fetch_log_files()
    print(f"[DEBUG] Znaleziono plików: {len(files)}")

    all_entries = []
    for filename in files:
        try:
            content = download_and_decode_log(ftp, filename)
            entries = parse_log_content(content, filename)
            insert_entries(entries)
            print(f"[INFO] Przetworzono: {filename} ({len(entries)} wpisów)")
            all_entries.extend(entries)
        except Exception as e:
            print(f"[ERROR] Błąd przy {filename}: {e}")

    ftp.quit()

    data = fetch_all_data()
    stats_df = build_stats(data)
    if stats_df is not None:
        message = format_table(stats_df)
        send_to_webhook(message)
    else:
        print("[INFO] Brak danych do wysłania.")

if __name__ == "__main__":
    main()
