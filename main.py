import ftplib
import io
import re
import psycopg2
import requests
import time
from collections import defaultdict
from tabulate import tabulate

# Konfiguracja FTP i bazy danych
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Inicjalizacja bazy
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            lock_type TEXT,
            success_count INTEGER,
            fail_count INTEGER,
            total_time FLOAT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_logs (
            filename TEXT PRIMARY KEY
        );
    """)
    conn.commit()
    conn.close()
    print("[DEBUG] Baza danych zainicjalizowana")

# Parsowanie pojedynczego wpisu
def parse_log_content(content):
    entries = []
    pattern = re.compile(
        r"User: (?P<nick>.*?) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>\d+\.\d+)\. Failed attempts: (?P<failed>\d+).*?Lock type: (?P<lock_type>\w+)",
        re.MULTILINE
    )

    for match in pattern.finditer(content):
        entries.append({
            "nick": match.group("nick").strip(),
            "success": match.group("success") == "Yes",
            "time": float(match.group("time")),
            "lock_type": match.group("lock_type")
        })

    return entries

# Pobieranie i przetwarzanie logów z FTP
def fetch_and_parse_logs():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")

    ftp.cwd(LOG_PATH)
    lines = []
    ftp.dir(lines.append)

    all_files = [line.split()[-1] for line in lines if "gameplay_" in line and line.endswith(".log")]
    new_entries = []

    for filename in all_files:
        cur.execute("SELECT 1 FROM processed_logs WHERE filename = %s", (filename,))
        if cur.fetchone():
            print(f"[INFO] Pominięto już przetworzony plik: {filename}")
            continue

        bio = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {filename}", bio.write)
        except Exception as e:
            print(f"[ERROR] Nie udało się pobrać pliku {filename}: {e}")
            continue

        content = bio.getvalue().decode('utf-16-le', errors='ignore')
        entries = parse_log_content(content)
        print(f"[DEBUG] Plik {filename} -> {len(entries)} wpisów")

        if entries:
            new_entries.extend(entries)
            cur.execute("INSERT INTO processed_logs (filename) VALUES (%s)", (filename,))

    conn.commit()
    conn.close()
    ftp.quit()
    return new_entries

# Aktualizacja danych w bazie
def update_database(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    stats = defaultdict(lambda: {"success": 0, "fail": 0, "time": 0.0})

    for entry in entries:
        key = (entry["nick"], entry["lock_type"])
        if entry["success"]:
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1
        stats[key]["time"] += entry["time"]

    for (nick, lock_type), data in stats.items():
        cur.execute("""
            SELECT success_count, fail_count, total_time
            FROM lockpick_stats
            WHERE nick = %s AND lock_type = %s
        """, (nick, lock_type))
        row = cur.fetchone()

        if row:
            new_success = row[0] + data["success"]
            new_fail = row[1] + data["fail"]
            new_time = row[2] + data["time"]
            cur.execute("""
                UPDATE lockpick_stats
                SET success_count = %s, fail_count = %s, total_time = %s
                WHERE nick = %s AND lock_type = %s
            """, (new_success, new_fail, new_time, nick, lock_type))
        else:
            cur.execute("""
                INSERT INTO lockpick_stats (nick, lock_type, success_count, fail_count, total_time)
                VALUES (%s, %s, %s, %s, %s)
            """, (nick, lock_type, data["success"], data["fail"], data["time"]))

    conn.commit()
    conn.close()
    print(f"[OK] Zaktualizowano {len(stats)} rekordów w bazie")

# Tworzenie tabeli wyników
def build_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT * FROM lockpick_stats")
    rows = cur.fetchall()
    conn.close()

    table_data = []
    for row in rows:
        nick, lock_type, succ, fail, total_time = row
        total = succ + fail
        effectiveness = f"{succ / total * 100:.1f}%" if total > 0 else "0%"
        avg_time = f"{total_time / succ:.2f}s" if succ > 0 else "-"
        table_data.append([nick, lock_type, total, succ, fail, effectiveness, avg_time])

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    return tabulate(table_data, headers, tablefmt="grid", stralign="center", numalign="center")

# Wysyłka tabeli na Discorda
def send_to_discord(message):
    payload = {"content": f"```\n{message}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[OK] Wysłano dane na Discorda")
    else:
        print(f"[ERROR] Nie udało się wysłać na Discorda: {response.status_code}")

# Główna pętla
def main_loop():
    print("[DEBUG] Start programu")
    init_db()
    while True:
        entries = fetch_and_parse_logs()
        if entries:
            print(f"[INFO] Znaleziono {len(entries)} nowych wpisów.")
            update_database(entries)
            table = build_table()
            send_to_discord(table)
        else:
            print("[INFO] Brak nowych wpisów.")
        time.sleep(60)

if __name__ == "__main__":
    main_loop()
