import os
import io
import ftplib
import re
import psycopg2
import requests
from tabulate import tabulate
from datetime import datetime
from flask import Flask

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
FORCE_WEBHOOK = True  # ❗ Wymuś wysyłkę niezależnie od nowych wpisów

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

def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            castle TEXT,
            result TEXT,
            time FLOAT,
            timestamp TEXT,
            UNIQUE(nick, castle, result, time, timestamp)
        )
    """)
    conn.commit()
    return conn, cur

def download_logs():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    try:
        files = ftp.mlsd()
        log_files = [name for name, facts in files if name.startswith("gameplay_") and name.endswith(".log")]
    except ftplib.error_perm:
        files = ftp.nlst()
        log_files = [name for name in files if name.startswith("gameplay_") and name.endswith(".log")]

    logs = []
    for filename in log_files:
        bio = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        content = bio.getvalue().decode("utf-16le", errors="ignore")
        logs.append(content)
    ftp.quit()
    return logs

def parse_log_entries(logs):
    pattern = re.compile(
        r"(?P<timestamp>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}).+?\[Lockpicking\] (?P<nick>.*?) tried to pick (?P<castle>.*?) lock and (?P<result>succeeded|failed) in (?P<time>\d+\.\d+)s"
    )
    entries = []
    for log in logs:
        for match in pattern.finditer(log):
            entries.append({
                "nick": match.group("nick"),
                "castle": match.group("castle"),
                "result": match.group("result"),
                "time": float(match.group("time")),
                "timestamp": match.group("timestamp")
            })
    return entries

def insert_new_entries(entries, cur, conn):
    new_count = 0
    for e in entries:
        try:
            cur.execute("""
                INSERT INTO lockpick_stats (nick, castle, result, time, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (e["nick"], e["castle"], e["result"], e["time"], e["timestamp"]))
            if cur.rowcount:
                new_count += 1
        except Exception as ex:
            print(f"[ERROR] Insert failed: {ex}")
    conn.commit()
    return new_count

def get_statistics(cur):
    cur.execute("SELECT nick, castle, result, time FROM lockpick_stats")
    rows = cur.fetchall()
    stats = {}
    for nick, castle, result, time in rows:
        key = (nick, castle)
        if key not in stats:
            stats[key] = {"success": 0, "fail": 0, "times": []}
        if result == "succeeded":
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1
        stats[key]["times"].append(time)
    return stats

def generate_table(stats):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    rows = []
    for (nick, castle), data in stats.items():
        total = data["success"] + data["fail"]
        success_rate = round((data["success"] / total) * 100, 2) if total else 0
        avg_time = round(sum(data["times"]) / len(data["times"]), 2) if data["times"] else 0
        rows.append([nick, castle, total, data["success"], data["fail"], f"{success_rate}%", f"{avg_time}s"])
    return "```\n" + tabulate(rows, headers, tablefmt="grid", stralign="center") + "\n```"

def send_to_webhook(content):
    payload = {"content": content}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[WEBHOOK] Status: {response.status_code}")

def main():
    print("[INFO] Inicjalizacja bazy...")
    conn, cur = connect_db()

    print("[INFO] Pobieranie logów...")
    logs = download_logs()
    print(f"[DEBUG] Liczba logów: {len(logs)}")

    print("[INFO] Parsowanie danych...")
    entries = parse_log_entries(logs)
    print(f"[DEBUG] Sparsowane wpisy lockpick: {len(entries)}")

    print("[INFO] Zapisywanie do bazy...")
    new_count = insert_new_entries(entries, cur, conn)
    print(f"[INFO] Nowe wpisy zapisane: {new_count}")

    if new_count > 0 or FORCE_WEBHOOK:
        print("[INFO] Generowanie statystyk...")
        stats = get_statistics(cur)
        if stats:
            table = generate_table(stats)
            print("[INFO] Wysyłanie do webhooka...")
            send_to_webhook(table)
        else:
            print("[WARN] Brak danych do wysłania.")
    else:
        print("[INFO] Brak nowych wpisów. Webhook nie został wysłany.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
    app.run(host="0.0.0.0", port=3000)
