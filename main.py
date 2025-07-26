import os
import ftplib
import psycopg2
import hashlib
import re
import io
import requests
from collections import defaultdict
from flask import Flask

# === KONFIGURACJA ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# === DANE LOGOWANIA DO NEON ===
DB_NAME = os.getenv("DB_NAME", "neondb")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "5432")

# === FLASK ALIVE ===
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# === POŁĄCZENIE Z BAZĄ ===
def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS lockpicks (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    lock_type TEXT,
                    success BOOLEAN,
                    time_sec FLOAT,
                    log_filename TEXT,
                    log_line_hash TEXT UNIQUE
                )
            ''')
            conn.commit()

# === PARSER LINII ===
line_pattern = re.compile(
    r"\[Lockpicking\] Player (?P<nick>.*?) (?P<result>succeeded|failed) to pick (?P<lock>\w+) lock in (?P<time>[\d\.]+) seconds"
)

def hash_line(line):
    return hashlib.sha256(line.encode('utf-8')).hexdigest()

def parse_log_file(name, content):
    entries = []
    for line in content.splitlines():
        match = line_pattern.search(line)
        if match:
            data = match.groupdict()
            time_str = data["time"].rstrip(".")  # Usunięcie kropki na końcu jeśli jest
            try:
                time_sec = float(time_str.replace(",", "."))
            except ValueError:
                print(f"[WARN] Nieprawidłowy format czasu: '{data['time']}' w linii: {line}")
                continue
            entries.append({
                "nick": data["nick"],
                "lock_type": data["lock"],
                "success": data["result"] == "succeeded",
                "time_sec": time_sec,
                "log_filename": name,
                "log_line_hash": hash_line(line)
            })
    return entries

# === POBIERZ LOGI ===
def fetch_logs_from_ftp():
    print("[INFO] Łączenie z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    print(f"[INFO] Zmieniono katalog na: {FTP_DIR}")

    print("[INFO] Pobieranie listy plików...")
    filenames = ftp.nlst("gameplay_*.log")
    print(f"[DEBUG] Znaleziono plików: {len(filenames)}")

    all_entries = []

    for filename in filenames:
        print(f"[INFO] Pobieranie pliku: {filename}")
        with io.BytesIO() as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
            try:
                content = f.getvalue().decode("utf-16-le", errors="ignore")
            except Exception as e:
                print(f"[ERROR] Błąd dekodowania pliku {filename}: {e}")
                continue
            print(f"[DEBUG] Rozmiar pliku {filename}: {len(content)} znaków")
            entries = parse_log_file(filename, content)
            print(f"[DEBUG] Sparsowano wpisów z pliku {filename}: {len(entries)}")
            all_entries.extend(entries)

    ftp.quit()
    print("[INFO] Zakończono pobieranie plików z FTP.")
    return all_entries

# === ZAPISZ DO BAZY ===
def save_new_entries(entries):
    new = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for e in entries:
                try:
                    cur.execute('''
                        INSERT INTO lockpicks (nick, lock_type, success, time_sec, log_filename, log_line_hash)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (log_line_hash) DO NOTHING
                    ''', (e['nick'], e['lock_type'], e['success'], e['time_sec'], e['log_filename'], e['log_line_hash']))
                    new += cur.rowcount
                except Exception as ex:
                    print(f"[ERROR] Błąd zapisu do bazy: {ex}")
            conn.commit()
    return new

# === STATYSTYKI ===
def compute_stats():
    stats = defaultdict(list)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nick, lock_type, success, time_sec FROM lockpicks")
            for nick, lock, success, time in cur.fetchall():
                stats[(nick, lock)].append((success, time))

    rows = []
    for (nick, lock), data in stats.items():
        total = len(data)
        successes = sum(1 for d in data if d[0])
        fails = total - successes
        accuracy = round(successes / total * 100, 1) if total > 0 else 0
        avg_time = round(sum(t for _, t in data) / total, 2)
        rows.append((nick, lock, total, successes, fails, f"{accuracy}%", avg_time))
    return rows

# === GENERUJ TABELĘ ===
def make_discord_table(rows):
    headers = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    cols = list(zip(*([headers] + rows)))
    col_widths = [max(len(str(cell)) for cell in col) for col in cols]

    def format_row(row):
        return "|" + "|".join(f" {str(cell).center(width)} " for cell, width in zip(row, col_widths)) + "|"

    table = "```\n" + format_row(headers) + "\n" + "|" + "|".join("-" * (w + 2) for w in col_widths) + "|\n"
    for row in rows:
        table += format_row(row) + "\n"
    return table + "```"

# === WYŚLIJ ===
def send_to_discord(table_text):
    try:
        response = requests.post(WEBHOOK_URL, json={"content": table_text})
        if response.status_code == 204:
            print("[INFO] Tabela wysłana na Discord.")
        else:
            print(f"[WARN] Odpowiedź Discorda: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[ERROR] Błąd podczas wysyłania na Discord: {e}")

# === MAIN ===
def main_loop():
    print("[INFO] Inicjalizacja bazy danych...")
    init_db()

    print("[INFO] Pobieranie logów z FTP...")
    entries = fetch_logs_from_ftp()
    print(f"[INFO] Znaleziono {len(entries)} wpisów")

    new = save_new_entries(entries)
    print(f"[INFO] Dodano {new} nowych wpisów do bazy")

    if new > 0:
        stats = compute_stats()
        table = make_discord_table(stats)
        send_to_discord(table)
    else:
        print("[INFO] Brak nowych danych do wysłania.")

if __name__ == "__main__":
    import threading
    threading.Thread(target=main_loop).start()
    app.run(host='0.0.0.0', port=3000)
