import os
import re
import time
import ftplib
import psycopg2
import requests
from io import BytesIO
from datetime import datetime
from flask import Flask
from threading import Thread

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_HOST = "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech"
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = "npg_dRU1YCtxbh6v"
DB_SSL = "require"

# --- INICJALIZACJA FLASK ---
app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

def run_flask():
    app.run(host="0.0.0.0", port=3000)

# --- BAZA DANYCH ---
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSL
    )

def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicks (
                    nick TEXT,
                    zamek TEXT,
                    wynik TEXT,
                    czas REAL,
                    data TIMESTAMP,
                    PRIMARY KEY (nick, zamek, data)
                )
            """)
            conn.commit()

# --- PRZETWARZANIE ---
def parse_log_content(content):
    pattern = re.compile(
        r"(?P<date>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}).*?Lockpicking:\s(?P<nick>.*?)\sattempted\sto\spick\s(?P<zamek>.*?)\s-\s(?P<wynik>SUCCESS|FAIL).*?in\s(?P<time>\d+[\.,]?\d*)s",
        re.DOTALL
    )
    entries = []
    for match in pattern.finditer(content):
        try:
            date_str = match.group("date").replace(".", "-").replace("-", " ", 2).replace("-", ":")
            timestamp = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            nick = match.group("nick").strip()
            zamek = match.group("zamek").strip()
            wynik = match.group("wynik")
            czas_str = match.group("time").replace(",", ".")
            czas = float(czas_str)
            entries.append((nick, zamek, wynik, czas, timestamp))
        except Exception as e:
            print(f"[ERROR] Błąd parsowania wpisu: {e}")
    print(f"[DEBUG] Znaleziono {len(entries)} wpisów w logu")
    return entries

def fetch_log_files():
    print("[INFO] Łączenie z FTP...")
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        files = ftp.nlst()
        log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
        print(f"[DEBUG] Znaleziono {len(log_files)} plików logów")

        all_entries = []
        for filename in log_files:
            try:
                bio = BytesIO()
                ftp.retrbinary(f"RETR {filename}", bio.write)
                content = bio.getvalue().decode("utf-16-le", errors="ignore")
                entries = parse_log_content(content)
                all_entries.extend(entries)
            except Exception as e:
                print(f"[ERROR] Błąd pobierania/parsing pliku {filename}: {e}")
        return all_entries

def insert_new_entries(entries):
    print("[INFO] Zapisywanie do bazy...")
    new_count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for entry in entries:
                try:
                    cur.execute("""
                        INSERT INTO lockpicks (nick, zamek, wynik, czas, data)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, entry)
                    if cur.rowcount > 0:
                        new_count += 1
            conn.commit()
    print(f"[DEBUG] Dodano {new_count} nowych wpisów")
    return new_count > 0

def generate_stats():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nick, zamek, wynik, czas FROM lockpicks")
            rows = cur.fetchall()

    stats = {}
    for nick, zamek, wynik, czas in rows:
        key = (nick, zamek)
        if key not in stats:
            stats[key] = {"total": 0, "success": 0, "fail": 0, "times": []}
        stats[key]["total"] += 1
        if wynik == "SUCCESS":
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1
        stats[key]["times"].append(czas)

    lines = [
        "| Nick | Zamek | Ilość prób | Udane | Nieudane | Skuteczność | Średni czas |",
        "|:----:|:-----:|:-----------:|:-----:|:--------:|:------------:|:-----------:|"
    ]
    for (nick, zamek), data in stats.items():
        skutecznosc = f"{(data['success'] / data['total']) * 100:.1f}%" if data["total"] else "0%"
        sredni = f"{sum(data['times']) / len(data['times']):.2f}s"
        lines.append(
            f"| {nick} | {zamek} | {data['total']} | {data['success']} | {data['fail']} | {skutecznosc} | {sredni} |"
        )
    return "\n".join(lines)

def send_to_discord(table):
    data = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=data)
    print(f"[INFO] Wysłano dane na webhook (status {response.status_code})")

# --- GŁÓWNA PĘTLA ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()

    while True:
        try:
            entries = fetch_log_files()
            if not entries:
                print("[INFO] Brak wpisów w logach.")
            elif insert_new_entries(entries):
                print("[INFO] Nowe dane — generuję tabelę...")
                table = generate_stats()
                send_to_discord(table)
            else:
                print("[INFO] Brak nowych wpisów.")
        except Exception as e:
            print(f"[ERROR] Błąd w pętli głównej: {e}")
        print("[DEBUG] Oczekiwanie 60s...")
        time.sleep(60)

# --- START ---
if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    main_loop()
