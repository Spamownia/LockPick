import re
import io
import time
import ftplib
import psycopg2
import pandas as pd
import requests
from tabulate import tabulate
from psycopg2 import sql
from psycopg2.extras import execute_values
from flask import Flask

# -- Konfiguracje --
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require",
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# -- Regex do parsowania linii logów --
LOG_PATTERN = re.compile(
    r"User: (?P<nick>.+?) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<elapsed>\d+\.\d+)\. "
    r"Failed attempts: (?P<fail>\d+)\. Target object: .+? Lock type: (?P<lock_type>.+?)\. "
)

app = Flask(__name__)

def init_db(conn):
    with conn.cursor() as cur:
        # Tworzenie tabeli jeśli nie istnieje
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicking_stats (
                id SERIAL PRIMARY KEY,
                nick TEXT NOT NULL,
                lock_type TEXT NOT NULL,
                attempts_total INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                fail_count INTEGER NOT NULL DEFAULT 0,
                elapsed_sum FLOAT NOT NULL DEFAULT 0,
                UNIQUE (nick, lock_type)
            );
        """)
        # Sprawdzenie, czy kolumna lock_type istnieje - w razie czego dodanie
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name='lockpicking_stats' AND column_name='lock_type';
        """)
        if cur.rowcount == 0:
            # Dodanie kolumny, jeśli brakuje (najpewniej niepotrzebne, bo wyżej tworzymy z lock_type)
            cur.execute("""
                ALTER TABLE lockpicking_stats ADD COLUMN lock_type TEXT NOT NULL DEFAULT 'Unknown';
            """)
        conn.commit()

def fetch_ftp_filelist(ftp):
    # Pobiera listę plików gameplay_*.log z FTP bez używania nlst (serwer może nie obsługiwać)
    filelist = []
    def append_line(line):
        # FTP LIST zwraca linie, filtrujemy pliki po nazwie
        parts = line.split()
        if len(parts) < 9:
            return
        filename = parts[-1]
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            filelist.append(filename)
    ftp.retrlines('LIST ' + FTP_PATH, append_line)
    return filelist

def parse_log_content(content):
    entries = []
    for line in content.splitlines():
        m = LOG_PATTERN.search(line)
        if m:
            nick = m.group("nick")
            lock_type = m.group("lock_type")
            success = m.group("success") == "Yes"
            elapsed = float(m.group("elapsed"))
            fail = int(m.group("fail"))
            attempts = 1 + fail  # Próby = udane + nieudane (failed attempts + 1 udana lub nieudana)
            success_count = 1 if success else 0
            fail_count = 0 if success else attempts
            entries.append({
                "nick": nick,
                "lock_type": lock_type,
                "attempts": attempts,
                "success_count": success_count,
                "fail_count": fail_count,
                "elapsed": elapsed,
            })
    return entries

def update_db(conn, entries):
    if not entries:
        return 0
    with conn.cursor() as cur:
        # Grupowanie danych do agregacji po (nick, lock_type)
        data_map = {}
        for e in entries:
            key = (e["nick"], e["lock_type"])
            if key not in data_map:
                data_map[key] = {
                    "attempts": 0,
                    "success_count": 0,
                    "fail_count": 0,
                    "elapsed_sum": 0.0
                }
            data_map[key]["attempts"] += e["attempts"]
            data_map[key]["success_count"] += e["success_count"]
            data_map[key]["fail_count"] += e["fail_count"]
            data_map[key]["elapsed_sum"] += e["elapsed"]

        sql_query = """
            INSERT INTO lockpicking_stats (nick, lock_type, attempts_total, success_count, fail_count, elapsed_sum)
            VALUES %s
            ON CONFLICT (nick, lock_type) DO UPDATE SET
                attempts_total = lockpicking_stats.attempts_total + EXCLUDED.attempts_total,
                success_count = lockpicking_stats.success_count + EXCLUDED.success_count,
                fail_count = lockpicking_stats.fail_count + EXCLUDED.fail_count,
                elapsed_sum = lockpicking_stats.elapsed_sum + EXCLUDED.elapsed_sum
        """
        values = [
            (nick, lock_type, d["attempts"], d["success_count"], d["fail_count"], d["elapsed_sum"])
            for (nick, lock_type), d in data_map.items()
        ]
        execute_values(cur, sql_query, values)
        conn.commit()
        return len(values)

def send_webhook_table(conn):
    df = pd.read_sql("SELECT nick, lock_type, attempts_total, success_count, fail_count, elapsed_sum FROM lockpicking_stats", conn)
    if df.empty:
        print("[INFO] Brak danych do wysłania.")
        return False

    # Obliczenia skuteczności i średniego czasu
    df["success_rate"] = (df["success_count"] / df["attempts_total"] * 100).round(2)
    df["avg_time"] = (df["elapsed_sum"] / df["attempts_total"]).round(2)

    df = df.rename(columns={
        "nick": "Nick",
        "lock_type": "Zamek",
        "attempts_total": "Próby",
        "success_count": "Udane",
        "fail_count": "Nieudane",
        "success_rate": "Skuteczność (%)",
        "avg_time": "Średni czas"
    })

    # Wyśrodkowanie tekstu i formatowanie tabeli
    table_str = tabulate(
        df,
        headers="keys",
        tablefmt="github",
        showindex=False,
        numalign="center",
        stralign="center"
    )

    payload = {
        "content": f"```{table_str}```"
    }
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[INFO] Tabela wysłana na webhook.")
        return True
    else:
        print(f"[ERROR] Błąd wysyłania webhook: {response.status_code} {response.text}")
        return False

def main():
    print("[DEBUG] Start programu")
    # Połączenie do bazy
    conn = psycopg2.connect(**DB_CONFIG)
    init_db(conn)

    # Połączenie do FTP
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")

    # Pobierz listę plików
    filelist = fetch_ftp_filelist(ftp)
    print(f"[DEBUG] Znaleziono plików: {len(filelist)}")

    all_entries = []
    for filename in filelist:
        print(f"[INFO] Pobieram plik: {filename}")
        bio = io.BytesIO()
        ftp.retrbinary(f"RETR {FTP_PATH}{filename}", bio.write)
        content = bio.getvalue().decode("utf-16le")
        entries = parse_log_content(content)
        print(f"[DEBUG] Znaleziono wpisów: {len(entries)}")
        all_entries.extend(entries)

    ftp.quit()

    print(f"[DEBUG] Wszystkich wpisów: {len(all_entries)}")
    updated_count = update_db(conn, all_entries)
    if updated_count > 0:
        send_webhook_table(conn)
    else:
        print("[INFO] Brak danych do wysłania.")

    conn.close()

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    main()
    app.run(host='0.0.0.0', port=3000)
