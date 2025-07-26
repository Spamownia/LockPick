import os
import re
import time
import ftplib
import psycopg2
import requests
import threading
from io import BytesIO
from flask import Flask

# --- Konfiguracja ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_HOST = "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech"
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = "npg_dRU1YCtxbh6v"
DB_PORT = 5432

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Flask: endpoint do monitorowania ---
app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

def run_flask():
    app.run(host="0.0.0.0", port=3000)

# --- Połączenie z bazą danych ---
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode="require"
    )

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicks (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    zamek TEXT,
                    wynik TEXT,
                    czas REAL,
                    unikalny_id TEXT UNIQUE
                )
            """)
            conn.commit()

# --- Pobieranie logów z FTP ---
def fetch_logs():
    logs = []
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)
        filenames = [fn for fn in ftp.nlst() if fn.startswith("gameplay_") and fn.endswith(".log")]
        print(f"[DEBUG] Liczba logów: {len(filenames)}")
        for filename in filenames:
            with BytesIO() as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
                logs.append(f.getvalue().decode("utf-16-le", errors="ignore"))
    return logs

# --- Parsowanie danych ---
LOG_PATTERN = re.compile(
    r'\[(?P<time>[\d.]+)s\]\s+(?P<nick>\S+)\s+\((?P<result>SUCCESS|FAILURE)\).*?Lock Type: (?P<lock_type>.*?)\b',
    re.DOTALL
)

def parse_logs(logs):
    results = []
    for content in logs:
        for match in LOG_PATTERN.finditer(content):
            nick = match.group("nick")
            result = match.group("result")
            zamek = match.group("lock_type").strip()
            time_str = match.group("time").strip().rstrip(".")
            try:
                czas = float(time_str)
            except ValueError:
                continue
            unikalny_id = f"{nick}_{zamek}_{result}_{czas}"
            results.append((nick, zamek, result, czas, unikalny_id))
    return results

# --- Zapis do bazy (unikalne) ---
def save_results(parsed):
    new_entries = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for nick, zamek, result, czas, uid in parsed:
                try:
                    cur.execute("""
                        INSERT INTO lockpicks (nick, zamek, wynik, czas, unikalny_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (unikalny_id) DO NOTHING
                    """, (nick, zamek, result, czas, uid))
                    new_entries += cur.rowcount
            conn.commit()
    return new_entries

# --- Generowanie i wysyłanie tabeli ---
def generate_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, zamek,
                    COUNT(*) AS wszystkie,
                    COUNT(*) FILTER (WHERE wynik = 'SUCCESS') AS udane,
                    COUNT(*) FILTER (WHERE wynik = 'FAILURE') AS nieudane,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE wynik = 'SUCCESS') / COUNT(*), 1) AS skutecznosc,
                    ROUND(AVG(czas), 2) AS sredni_czas
                FROM lockpicks
                GROUP BY nick, zamek
                ORDER BY skutecznosc DESC
            """)
            rows = cur.fetchall()

    headers = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [len(h) for h in headers]

    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    def format_row(row):
        return " | ".join(str(val).center(col_widths[i]) for i, val in enumerate(row))

    header = format_row(headers)
    separator = "-+-".join("-" * w for w in col_widths)
    lines = [header, separator] + [format_row(row) for row in rows]
    return "```\n" + "\n".join(lines) + "\n```"

def send_webhook(content):
    response = requests.post(WEBHOOK_URL, json={"content": content})
    if response.status_code != 204:
        print(f"[ERROR] Nie udało się wysłać webhooka: {response.status_code}")

# --- Główna pętla ---
def main_loop():
    print("[INFO] Inicjalizacja bazy danych...")
    init_db()
    while True:
        print("[INFO] Pobieranie logów...")
        try:
            logs = fetch_logs()
            print("[INFO] Parsowanie danych...")
            parsed = parse_logs(logs)
            print(f"[INFO] Liczba wpisów do sprawdzenia: {len(parsed)}")
            new = save_results(parsed)
            if new > 0:
                print(f"[INFO] Dodano nowych wpisów: {new}")
                tabela = generate_table()
                send_webhook(tabela)
            else:
                print("[INFO] Brak nowych wpisów w logach.")
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(60)

# --- Start ---
if __name__ == "__main__":
    threading.Thread(target=main_loop).start()
    run_flask()
