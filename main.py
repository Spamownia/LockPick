# main.py
import os
import re
import ftplib
import psycopg2
import requests
from tabulate import tabulate
from flask import Flask

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def initialize_db(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            castle TEXT,
            result TEXT,
            time FLOAT
        )
    """)
    # Dodanie kolumny 'result' jeśli jej nie ma
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'lockpick_stats' AND column_name = 'result'
    """)
    if not cur.fetchone():
        print("[INFO] Dodawanie brakującej kolumny 'result'...")
        cur.execute("ALTER TABLE lockpick_stats ADD COLUMN result TEXT")

def download_logs():
    print("[INFO] Pobieranie logów...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    all_files = []
    try:
        ftp.retrlines('LIST', lambda x: all_files.append(x.split()[-1]))
    except Exception as e:
        print(f"[ERROR] Nie można pobrać listy plików: {e}")
        return []

    logs = []
    for filename in all_files:
        if filename.startswith("gameplay_") and filename.endswith(".log"):
            from io import BytesIO
            content = BytesIO()
            try:
                ftp.retrbinary(f"RETR {filename}", content.write)
                decoded = content.getvalue().decode("utf-16-le", errors="ignore")
                logs.append(decoded)
            except Exception as e:
                print(f"[ERROR] Nie można pobrać {filename}: {e}")
    ftp.quit()
    print(f"[DEBUG] Liczba logów: {len(logs)}")
    return logs

def parse_logs(logs):
    print("[INFO] Parsowanie danych...")
    pattern = re.compile(
        r"(?P<timestamp>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): .*?Character '(?P<nick>[^']+)' .*?started lockpicking on (?P<castle>\w+).*?"
        r"(?:(?:resulted in (?P<result>SUCCESS|FAILURE))|(?:failed with error)).*?time: (?P<time>\d+\.\d+)", 
        re.DOTALL
    )
    entries = []
    for log in logs:
        for match in pattern.finditer(log):
            entries.append({
                "nick": match.group("nick"),
                "castle": match.group("castle"),
                "result": match.group("result") or "FAILURE",
                "time": float(match.group("time"))
            })
    print(f"[DEBUG] Sparsowane wpisy lockpick: {len(entries)}")
    return entries

def save_to_db(cur, entries):
    print("[INFO] Zapisywanie do bazy...")
    new_count = 0
    for entry in entries:
        cur.execute("""
            SELECT 1 FROM lockpick_stats 
            WHERE nick=%s AND castle=%s AND result=%s AND time=%s
        """, (entry["nick"], entry["castle"], entry["result"], entry["time"]))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO lockpick_stats (nick, castle, result, time)
                VALUES (%s, %s, %s, %s)
            """, (entry["nick"], entry["castle"], entry["result"], entry["time"]))
            new_count += 1
    print(f"[INFO] Nowe wpisy zapisane: {new_count}")
    return new_count

def get_statistics(cur):
    print("[INFO] Generowanie statystyk...")
    cur.execute("SELECT nick, castle, result, time FROM lockpick_stats")
    data = cur.fetchall()
    stats = {}
    for nick, castle, result, time in data:
        key = (nick, castle)
        if key not in stats:
            stats[key] = {"total": 0, "success": 0, "fail": 0, "times": []}
        stats[key]["total"] += 1
        stats[key]["success" if result == "SUCCESS" else "fail"] += 1
        stats[key]["times"].append(time)
    
    table = []
    for (nick, castle), values in stats.items():
        total = values["total"]
        success = values["success"]
        fail = values["fail"]
        avg_time = round(sum(values["times"]) / total, 2)
        skutecznosc = f"{(success / total * 100):.0f}%"
        table.append([nick, castle, total, success, fail, skutecznosc, avg_time])
    
    return sorted(table, key=lambda row: (-row[3], row[5]))  # sortuj po sukcesach, potem skuteczności

def send_to_webhook(table):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    msg = "```" + tabulate(table, headers=headers, tablefmt="pretty") + "```"
    print("[INFO] Wysyłanie do webhook...")
    requests.post(WEBHOOK_URL, json={"content": msg})

def main():
    print("[INFO] Inicjalizacja bazy...")
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    initialize_db(cur)

    logs = download_logs()
    if not logs:
        print("[INFO] Brak logów.")
        return

    entries = parse_logs(logs)
    if not entries:
        print("[INFO] Brak danych lockpick.")
        return

    new = save_to_db(cur, entries)
    if new > 0:
        stats = get_statistics(cur)
        send_to_webhook(stats)
    else:
        print("[INFO] Brak nowych danych do wysłania.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
    app.run(host="0.0.0.0", port=3000)
