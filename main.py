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

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOGS_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_HOST = "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech"
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASS = "npg_dRU1YCtxbh6v"
DB_SSLMODE = "require"

# --- FLASK DO UPTIMEROBOT ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- BAZA DANYCH ---
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        sslmode=DB_SSLMODE
    )

def init_db():
    print("[INFO] Inicjalizacja bazy danych...")
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS lockpicks (
                        nick TEXT,
                        castle TEXT,
                        result TEXT,
                        time FLOAT,
                        logname TEXT
                    )
                """)
            conn.commit()
    except Exception as e:
        print(f"[ERROR] Błąd inicjalizacji DB: {e}")

# --- PARSER ---
def parse_log_file(file_content, logname):
    results = []
    lines = file_content.splitlines()
    pattern = re.compile(r"Lockpicking minigame result for (?P<nick>\w+): (?P<result>Success|Failure) in (?P<time>[\d.]+) seconds \(Castle (?P<castle>\w+)\)")
    for line in lines:
        match = pattern.search(line)
        if match:
            try:
                nick = match.group("nick")
                result = match.group("result")
                time_val = float(match.group("time").replace(",", "."))
                castle = match.group("castle")
                results.append((nick, castle, result, time_val, logname))
            except Exception as e:
                print(f"[WARN] Nieprawidłowy wpis w {logname}: {e}")
    return results

# --- FTP ---
def fetch_log_files():
    print("[INFO] Łączenie z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOGS_PATH)
    files = ftp.nlst()
    log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[INFO] Znaleziono {len(log_files)} plików logów.")
    all_data = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT logname FROM lockpicks")
                existing_logs = {row[0] for row in cur.fetchall()}
    except Exception as e:
        print(f"[ERROR] Błąd odczytu istniejących logów: {e}")
        return []

    for filename in log_files:
        if filename in existing_logs:
            print(f"[DEBUG] Pomijam istniejący plik: {filename}")
            continue
        print(f"[INFO] Przetwarzanie: {filename}")
        r = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", r.write)
        r.seek(0)
        try:
            text = codecs.decode(r.read(), "utf-16-le")
            entries = parse_log_file(text, filename)
            all_data.extend(entries)
        except Exception as e:
            print(f"[ERROR] Błąd dekodowania {filename}: {e}")
    ftp.quit()
    return all_data

# --- ZAPIS DO BAZY ---
def save_data(entries):
    if not entries:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO lockpicks (nick, castle, result, time, logname)
                    VALUES (%s, %s, %s, %s, %s)
                """, entries)
            conn.commit()
        print(f"[INFO] Zapisano {len(entries)} wpisów do bazy.")
    except Exception as e:
        print(f"[ERROR] Błąd zapisu do DB: {e}")

# --- STATYSTYKI ---
def compute_stats():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nick, castle, result, time FROM lockpicks")
            rows = cur.fetchall()
    stats = {}
    for nick, castle, result, time_val in rows:
        key = (nick, castle)
        if key not in stats:
            stats[key] = {'success': 0, 'fail': 0, 'times': []}
        if result == "Success":
            stats[key]['success'] += 1
        else:
            stats[key]['fail'] += 1
        stats[key]['times'].append(time_val)
    return stats

# --- FORMATOWANIE I WYSYŁKA ---
def send_to_discord(stats):
    if not stats:
        print("[INFO] Brak nowych danych.")
        return
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    rows = []
    for (nick, castle), data in stats.items():
        total = data['success'] + data['fail']
        success_rate = f"{(data['success'] / total) * 100:.1f}%"
        avg_time = f"{sum(data['times']) / len(data['times']):.2f}s"
        rows.append([nick, castle, str(total), str(data['success']), str(data['fail']), success_rate, avg_time])

    # Automatyczne dopasowanie szerokości kolumn
    col_widths = [max(len(row[i]) for row in ([headers] + rows)) for i in range(len(headers))]
    def format_row(row): return "| " + " | ".join(row[i].center(col_widths[i]) for i in range(len(row))) + " |"

    table = "\n".join([
        format_row(headers),
        "|-" + "-|-".join("-" * col_widths[i] for i in range(len(headers))) + "-|",
        *[format_row(row) for row in rows]
    ])

    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table}\n```"})
    if response.ok:
        print("[INFO] Wysłano statystyki do Discorda.")
    else:
        print(f"[ERROR] Nie udało się wysłać: {response.status_code}")

# --- PĘTLA GŁÓWNA ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        new_entries = fetch_log_files()
        if new_entries:
            save_data(new_entries)
            stats = compute_stats()
            send_to_discord(stats)
        else:
            print("[INFO] Brak nowych zdarzeń w logach.")
        time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
