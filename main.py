import os
import time
import ftplib
import re
import threading
from io import BytesIO
from datetime import datetime
from collections import defaultdict
from flask import Flask
import requests
import psycopg2
import hashlib

# === Dane FTP ===
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# === Discord Webhooki ===
DISCORD_WEBHOOK_FULL = 'https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3'
DISCORD_WEBHOOK_SHORT = 'https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3'
DISCORD_WEBHOOK_PODIUM = 'https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3'

# === Kolejność zamków ===
LOCK_ORDER = ['VeryEasy', 'Basic', 'Medium', 'Advanced', 'DialLock']

# === Statystyki globalne ===
stats = defaultdict(lambda: defaultdict(lambda: {
    'all': 0,
    'success': 0,
    'fail': 0,
    'total_time': 0.0
}))

last_log = None
already_deployed_once = False

# --- Konfiguracja bazy Neon ---
DB_CONFIG = {
    'host': 'ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech',
    'database': 'neondb',
    'user': 'neondb_owner',
    'password': 'npg_dRU1YCtxbh6v',
    'sslmode': 'require'
}

def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        sslmode=DB_CONFIG['sslmode']
    )

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                hash TEXT UNIQUE,
                user_nick TEXT,
                lock_type TEXT,
                success BOOLEAN,
                elapsed FLOAT,
                fail_count INT,
                raw_line TEXT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """)
        conn.commit()

def line_hash(line: str) -> str:
    return hashlib.sha256(line.encode('utf-8')).hexdigest()

def is_line_processed(line: str) -> bool:
    h = line_hash(line)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM logs WHERE hash = %s", (h,))
            return cur.fetchone() is not None

def mark_line_processed(line: str, user, lock_type, success, elapsed, fail_count):
    h = line_hash(line)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs(hash, user_nick, lock_type, success, elapsed, fail_count, raw_line)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (hash) DO NOTHING
            """, (h, user, lock_type, success, elapsed, fail_count, line))
        conn.commit()

def load_stats_from_db():
    global stats
    stats.clear()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_nick, lock_type, success, elapsed, fail_count FROM logs")
            rows = cur.fetchall()
            for user, lock, success, elapsed, fail_count in rows:
                stat = stats[user][lock]
                if success:
                    stat['all'] += 1 + fail_count
                    stat['success'] += 1
                    stat['fail'] += fail_count
                    stat['total_time'] += elapsed
                else:
                    stat['all'] += fail_count
                    stat['fail'] += fail_count

def get_already_deployed_once() -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM state WHERE key = 'already_deployed_once'")
            row = cur.fetchone()
            if row:
                return row[0] == '1'
    return False

def set_already_deployed_once(value: bool):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO state(key, value) VALUES ('already_deployed_once', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, ('1' if value else '0',))
        conn.commit()

def parse_log_line(line):
    match = re.search(r'User: (.+?) \([0-9, ]+\).*?Success: (Yes|No).*?Elapsed time: ([\d.]+).*?Failed attempts: (\d+).*?Lock type: (\w+)', line)
    if match:
        user = match.group(1).strip()
        success = match.group(2) == "Yes"
        elapsed_str = match.group(3).rstrip('.')
        try:
            elapsed = float(elapsed_str)
        except ValueError:
            return None
        failed_attempts = int(match.group(4))
        lock_type = match.group(5)
        return user, lock_type, success, elapsed, failed_attempts
    return None

def process_line(line):
    parsed = parse_log_line(line)
    if not parsed:
        return False
    user, lock, success, elapsed, fail_count = parsed
    if is_line_processed(line):
        return False
    # Zapis do bazy i lokalna aktualizacja stats
    mark_line_processed(line, user, lock, success, elapsed, fail_count)
    stat = stats[user][lock]
    if success:
        stat['all'] += 1 + fail_count
        stat['success'] += 1
        stat['fail'] += fail_count
        stat['total_time'] += elapsed
    else:
        stat['all'] += fail_count
        stat['fail'] += fail_count
    return True

def fetch_logs():
    global last_log
    logs = []
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOG_DIR)
            files = []
            def parse_list_line(line):
                parts = line.split()
                if len(parts) >= 9:
                    filename = parts[-1]
                    files.append(filename)
            ftp.retrlines('LIST', parse_list_line)
            log_files = sorted([f for f in files if f.startswith('gameplay_') and f.endswith('.log')])
            for filename in log_files:
                bio = BytesIO()
                ftp.retrbinary(f"RETR {filename}", bio.write)
                content = bio.getvalue().decode('utf-16le', errors='ignore')
                logs.append((filename, content))
            if log_files:
                last_log = log_files[-1]
    except Exception as e:
        print(f"[ERROR] Błąd FTP podczas pobierania listy plików: {e}")
    return logs

def generate_full_table():
    headers = ['Nick', 'Zamek', 'Wszystkie', 'Udane', 'Nieudane', 'Skuteczność', 'Średni czas']
    col_widths = [max(len(h), 10) for h in headers]
    rows = []
    for user in sorted(stats.keys()):
        for lock in LOCK_ORDER:
            data = stats[user].get(lock)
            if not data or data['all'] == 0:
                continue
            skutecznosc = f"{(data['success'] / data['all']) * 100:.2f}%"
            avg_time = f"{(data['total_time'] / data['success']):.2f}s" if data['success'] else "-"
            row = [user, lock, str(data['all']), str(data['success']), str(data['fail']), skutecznosc, avg_time]
            rows.append(row)
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(cell))
    header = "| " + " | ".join(h.center(col_widths[i]) for i, h in enumerate(headers)) + " |"
    separator = "|-" + "-|-".join("-" * col_widths[i] for i in range(len(headers))) + "-|"
    table_rows = ["| " + " | ".join(row[i].center(col_widths[i]) for i in range(len(row))) + " |" for row in rows]
    return "\n".join([header, separator] + table_rows) if rows else "| Brak danych |"

def generate_short_table():
    headers = ['Nick', 'Zamek', 'Skuteczność', 'Średni czas']
    col_widths = [max(len(h), 10) for h in headers]
    rows = []
    for user in sorted(stats.keys()):
        for lock in LOCK_ORDER:
            data = stats[user].get(lock)
            if not data or data['all'] == 0:
                continue
            skutecznosc = f"{(data['success'] / data['all']) * 100:.2f}%"
            avg_time = f"{(data['total_time'] / data['success']):.2f}s" if data['success'] else "-"
            row = [user, lock, skutecznosc, avg_time]
            rows.append(row)
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(cell))
    header = "| " + " | ".join(h.center(col_widths[i]) for i, h in enumerate(headers)) + " |"
    separator = "|-" + "-|-".join("-" * col_widths[i] for i in range(len(headers))) + "-|"
    table_rows = ["| " + " | ".join(row[i].center(col_widths[i]) for i in range(len(row))) + " |" for row in rows]
    return "\n".join([header, separator] + table_rows) if rows else "| Brak danych |"

def generate_podium_table():
    ranking = []
    for user in stats:
        total_success = sum(stats[user][lock]['success'] for lock in LOCK_ORDER)
        total_all = sum(stats[user][lock]['all'] for lock in LOCK_ORDER)
        if total_all == 0:
            continue
        skutecznosc = (total_success / total_all) * 100
        ranking.append((user, skutecznosc))
    ranking.sort(key=lambda x: x[1], reverse=True)
    headers = ['🏅', 'Nick', 'Skuteczność']
    col_widths = [2, 10, 12]
    rows = []
    for idx, (user, skutecznosc) in enumerate(ranking):
        emoji = "🥇" if idx == 0 else "🥈" if idx == 1 else "🥉" if idx == 2 else str(idx + 1)
        row = [emoji, user, f"{skutecznosc:.2f}%"]
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))
        rows.append(row)
    header = "| " + " | ".join(headers[i].center(col_widths[i]) for i in range(3)) + " |"
    separator = "|-" + "-|-".join("-" * col_widths[i] for i in range(3)) + "-|"
    table_rows = ["| " + " | ".join(row[i].center(col_widths[i]) for i in range(3)) + " |" for row in rows]
    return "\n".join([header, separator] + table_rows) if rows else "| Brak danych |"

def send_to_discord(table_full, table_short, table_podium):
    webhooks = [
        (DISCORD_WEBHOOK_FULL, table_full),
        (DISCORD_WEBHOOK_SHORT, table_short),
        (DISCORD_WEBHOOK_PODIUM, table_podium),
    ]
    for url, content in webhooks:
        data = { "content": "```\n" + content + "\n```" }
        try:
            r = requests.post(url, json=data, timeout=10)
            if r.status_code != 204:
                print(f"[ERROR] Discord HTTP {r.status_code}: {r.text}")
        except Exception as e:
            print(f"[ERROR] Discord post failed: {e}")

def process_all_logs():
    print("🔁 Uruchamianie pełnego przetwarzania logów...")
    init_db()
    logs = fetch_logs()
    any_new = False
    for _, content in logs:
        for line in content.splitlines():
            if process_line(line):
                any_new = True
    load_stats_from_db()
    print("[INFO] Przetworzono wszystkie dostępne logi.")
    global already_deployed_once
    already_deployed_once = get_already_deployed_once()
    if not already_deployed_once and any_new:
        table_full = generate_full_table()
        table_short = generate_short_table()
        table_podium = generate_podium_table()
        print(table_full)
        print(table_short)
        print(table_podium)
        send_to_discord(table_full, table_short, table_podium)
        set_already_deployed_once(True)
        already_deployed_once = True

def background_worker():
    global last_log
    print("🔁 Start wątku do monitorowania nowych linii w najnowszym pliku...")
    while True:
        try:
            logs = fetch_logs()
            current_log = None
            for fname, content in logs:
                if fname == last_log:
                    current_log = content
                    break
            if current_log:
                new_lines_count = 0
                for line in current_log.splitlines():
                    if process_line(line):
                        new_lines_count += 1
                if new_lines_count > 0:
                    print(f"[INFO] Wykryto {new_lines_count} nowych wpisów.")
                    load_stats_from_db()
                    table_full = generate_full_table()
                    table_short = generate_short_table()
                    table_podium = generate_podium_table()
                    print(table_full)
                    print(table_short)
                    print(table_podium)
                    send_to_discord(table_full, table_short, table_podium)
        except Exception as e:
            print(f"[ERROR] Błąd w tle: {e}")
        time.sleep(60)

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    return "Lockpicking stat collector is running."

if __name__ == "__main__":
    process_all_logs()
    threading.Thread(target=background_worker, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
