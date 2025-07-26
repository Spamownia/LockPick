import os
import re
import time
import psycopg2
import requests
from ftplib import FTP
from datetime import datetime
from io import BytesIO
from threading import Thread
from flask import Flask

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

# --- FLASK ALIVE ---
app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

Thread(target=app.run, kwargs={"host": "0.0.0.0", "port": 3000}).start()

# --- BAZA ---
def get_conn():
    return psycopg2.connect(
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        sslmode=DB_CONFIG["sslmode"]
    )

def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicks (
                    nick TEXT,
                    castle TEXT,
                    result TEXT,
                    time FLOAT,
                    log_file TEXT,
                    UNIQUE(nick, castle, result, time, log_file)
                )
            """)
        conn.commit()

# --- PARSER LOCKPICKÓW ---
lockpick_regex = re.compile(
    r"Lockpicking: (?P<nick>.+?) tried to pick the (?P<castle>.+?) lock and (?P<result>succeeded|failed) in (?P<time>\d+,\d+|\d+\.\d+?)s"
)

def parse_log_file(log_data, file_name):
    entries = []
    for match in lockpick_regex.finditer(log_data):
        nick = match.group("nick")
        castle = match.group("castle")
        result = match.group("result")
        raw_time = match.group("time").replace(",", ".").replace("..", ".").strip(".")
        try:
            time_val = float(raw_time)
        except ValueError:
            print(f"[WARN] Nieprawidłowy czas w {file_name}: {raw_time}")
            continue
        entries.append((nick, castle, result, time_val, file_name))
    print(f"[DEBUG] W pliku {file_name} znaleziono {len(entries)} wpisów lockpick.")
    return entries

# --- FTP POBIERANIE ---
def fetch_log_files():
    print("[DEBUG] Łączenie z FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    filenames = ftp.nlst()
    log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików logów.")
    files_data = []
    for filename in log_files:
        print(f"[DEBUG] Pobieranie pliku: {filename}")
        bio = BytesIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        content = bio.getvalue().decode("utf-16le", errors="ignore")
        files_data.append((filename, content))
    ftp.quit()
    return files_data

# --- ZAPIS DO BAZY ---
def insert_entries(entries):
    new_count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for entry in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpicks (nick, castle, result, time, log_file)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, entry)
                    if cur.rowcount > 0:
                        new_count += 1
                except Exception as e:
                    print(f"[ERROR] Błąd zapisu: {e}")
        conn.commit()
    print(f"[DEBUG] Dodano {new_count} nowych wpisów do bazy.")
    return new_count

# --- ANALIZA I TABELA ---
def analyze_data():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, castle,
                       COUNT(*) as total,
                       COUNT(*) FILTER (WHERE result = 'succeeded') as success,
                       COUNT(*) FILTER (WHERE result = 'failed') as fail,
                       ROUND(AVG(time), 2) as avg_time
                FROM lockpicks
                GROUP BY nick, castle
                ORDER BY success DESC, avg_time ASC
            """)
            rows = cur.fetchall()
    if not rows:
        return None

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = [headers]
    for row in rows:
        nick, castle, total, success, fail, avg_time = row
        effectiveness = f"{round((success / total) * 100)}%"
        table.append([
            nick, castle, str(total), str(success), str(fail), effectiveness, f"{avg_time}s"
        ])

    # Oblicz maksymalną szerokość kolumny
    col_widths = [max(len(str(item)) for item in col) for col in zip(*table)]

    lines = []
    for row in table:
        line = " | ".join(str(val).center(width) for val, width in zip(row, col_widths))
        lines.append(line)

    return "```\n" + "\n".join(lines) + "\n```"

def send_to_discord(content):
    if content:
        print("[DEBUG] Wysyłanie tabeli na webhook...")
        requests.post(WEBHOOK_URL, json={"content": content})

# --- GŁÓWNA PĘTLA ---
def main_loop():
    init_db()
    seen_files = set()
    while True:
        try:
            all_entries = []
            files = fetch_log_files()
            for file_name, content in files:
                if file_name in seen_files:
                    continue
                entries = parse_log_file(content, file_name)
                if entries:
                    inserted = insert_entries(entries)
                    if inserted > 0:
                        all_entries.extend(entries)
                seen_files.add(file_name)

            if all_entries:
                tabela = analyze_data()
                send_to_discord(tabela)
            else:
                print("[DEBUG] Brak nowych wpisów.")

        except Exception as e:
            print(f"[ERROR] Błąd główny: {e}")

        time.sleep(60)

if __name__ == "__main__":
    Thread(target=main_loop).start()
