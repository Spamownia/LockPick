import ftplib
import os
import re
import psycopg2
import requests
import threading
import time
import io
import codecs
from datetime import datetime
from flask import Flask

# ====== Konfiguracja ======
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

# ====== Inicjalizacja aplikacji Flask ======
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# ====== Funkcja połączenia z bazą danych ======
def get_conn():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        sslmode=DB_CONFIG["sslmode"]
    )

# ====== Inicjalizacja bazy danych ======
def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_stats (
                    nick TEXT,
                    zamek TEXT,
                    success BOOLEAN,
                    duration FLOAT,
                    timestamp TIMESTAMP,
                    UNIQUE(nick, zamek, timestamp)
                )
            """)
            conn.commit()

# ====== Parsowanie linii logów ======
def parse_log_line(line):
    match = re.search(r'\[(.*?)\] \[Lockpicking\] (.*?) tried to pick lock (.*?) - (SUCCESS|FAILED) in ([0-9.]+)s', line)
    if match:
        timestamp_str, nick, zamek, status, duration = match.groups()
        return {
            "timestamp": datetime.strptime(timestamp_str, "%Y.%m.%d-%H.%M.%S"),
            "nick": nick,
            "zamek": zamek,
            "success": status == "SUCCESS",
            "duration": float(duration)
        }
    return None

# ====== Pobieranie listy plików z FTP ======
def list_log_files(ftp):
    print("[DEBUG] Pobieranie listy plików logów z FTP...")
    files = []

    def parse_line(line):
        parts = line.split()
        filename = parts[-1]
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            files.append(filename)

    ftp.dir(LOG_DIR, parse_line)
    return files

# ====== Pobieranie i parsowanie plików logów ======
def fetch_log_files():
    print("[DEBUG] Łączenie z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)

    files = list_log_files(ftp)
    print(f"[DEBUG] Znaleziono {len(files)} plików.")

    entries = []

    for filename in files:
        print(f"[INFO] Przetwarzanie: {filename}")
        log_io = io.BytesIO()
        ftp.retrbinary(f"RETR {LOG_DIR}/{filename}", log_io.write)
        log_io.seek(0)
        decoded = codecs.decode(log_io.read(), 'utf-16-le')
        for line in decoded.splitlines():
            parsed = parse_log_line(line)
            if parsed:
                entries.append(parsed)

    ftp.quit()
    print(f"[INFO] Wczytano {len(entries)} wpisów z logów.")
    return entries

# ====== Zapis nowych wpisów do bazy danych ======
def save_entries(entries):
    print("[DEBUG] Zapis danych do bazy...")
    new_entries = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for e in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpick_stats (nick, zamek, success, duration, timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (e["nick"], e["zamek"], e["success"], e["duration"], e["timestamp"]))
                    new_entries += cur.rowcount
                except Exception as ex:
                    print(f"[ERROR] Błąd zapisu: {ex}")
        conn.commit()
    print(f"[INFO] Dodano {new_entries} nowych wpisów.")
    return new_entries

# ====== Generowanie tabeli do Discorda ======
def generate_table():
    print("[DEBUG] Generowanie tabeli...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, zamek,
                    COUNT(*) AS ilosc_proby,
                    COUNT(*) FILTER (WHERE success) AS udane,
                    COUNT(*) FILTER (WHERE NOT success) AS nieudane,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE success) / COUNT(*), 2) AS skutecznosc,
                    ROUND(AVG(duration), 2) AS sredni_czas
                FROM lockpick_stats
                GROUP BY nick, zamek
                ORDER BY skutecznosc DESC, udane DESC
            """)
            rows = cur.fetchall()

    if not rows:
        return None

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]

    table = "```\n"
    table += " | ".join(header.center(col_widths[i]) for i, header in enumerate(headers)) + "\n"
    table += "-+-".join("-" * col_width for col_width in col_widths) + "\n"
    for row in rows:
        table += " | ".join(str(row[i]).center(col_widths[i]) for i in range(len(headers))) + "\n"
    table += "```"
    return table

# ====== Wysyłanie wiadomości ======
def send_to_discord(message):
    print("[DEBUG] Wysyłanie danych do Discord...")
    data = {"content": message}
    response = requests.post(WEBHOOK_URL, json=data)
    print(f"[INFO] Webhook status: {response.status_code}")

# ====== Główna pętla ======
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        try:
            entries = fetch_log_files()
            added = save_entries(entries)
            if added > 0:
                table = generate_table()
                if table:
                    send_to_discord(table)
            else:
                print("[INFO] Brak nowych danych.")
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(60)

# ====== Uruchomienie pętli w tle ======
threading.Thread(target=main_loop, daemon=True).start()

# ====== Start serwera Flask ======
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
