# main.py

import os
import re
import ftplib
import psycopg2
import requests
from io import BytesIO
from datetime import datetime
from tabulate import tabulate
from flask import Flask

# FTP dane
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# Webhook
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# DB config
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

def get_ftp_log_files():
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        files = ftp.nlst()
        return [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

def download_log_file(filename):
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        buffer = BytesIO()
        ftp.retrbinary(f"RETR {filename}", buffer.write)
        buffer.seek(0)
        return buffer.read().decode("utf-16-le", errors="ignore")

def parse_log_line(line):
    pattern = re.compile(
        r"User: (?P<nick>\w+) \(\d+, (?P<steam_id>\d+)\)\. "
        r"Success: (?P<result>Yes|No)\. "
        r"Elapsed time: (?P<time>[\d.]+)\. "
        r"Failed attempts: (?P<fail>\d+)\. "
        r"Target object: (?P<castle>[^\(]+)\(ID: [^)]+\)\. "
        r"Lock type: (?P<lock_type>\w+)\. "
    )
    match = pattern.search(line)
    if match:
        d = match.groupdict()
        return {
            "nick": d["nick"],
            "castle": d["castle"].strip(),
            "result": d["result"],
            "time": float(d["time"])
        }
    return None

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpick_stats (
                id SERIAL PRIMARY KEY,
                nick TEXT,
                castle TEXT,
                result TEXT,
                time FLOAT
            )
        """)
        conn.commit()

        # Upewnij się, że kolumna result istnieje
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='lockpick_stats'")
        columns = [row[0] for row in cur.fetchall()]
        if "result" not in columns:
            print("[INFO] Dodawanie brakującej kolumny 'result'...")
            cur.execute("ALTER TABLE lockpick_stats ADD COLUMN result TEXT")
            conn.commit()

def insert_new_entries(conn, entries):
    new_count = 0
    with conn.cursor() as cur:
        for entry in entries:
            cur.execute("""
                SELECT 1 FROM lockpick_stats
                WHERE nick = %s AND castle = %s AND result = %s AND time = %s
            """, (entry["nick"], entry["castle"], entry["result"], entry["time"]))
            if cur.fetchone() is None:
                cur.execute("""
                    INSERT INTO lockpick_stats (nick, castle, result, time)
                    VALUES (%s, %s, %s, %s)
                """, (entry["nick"], entry["castle"], entry["result"], entry["time"]))
                new_count += 1
        conn.commit()
    return new_count

def get_statistics(cur):
    cur.execute("SELECT nick, castle, result, time FROM lockpick_stats")
    rows = cur.fetchall()

    stats = {}
    for nick, castle, result, time in rows:
        key = (nick, castle)
        if key not in stats:
            stats[key] = {"total": 0, "success": 0, "fail": 0, "times": []}
        stats[key]["total"] += 1
        if result.lower() == "yes":
            stats[key]["success"] += 1
            stats[key]["times"].append(time)
        else:
            stats[key]["fail"] += 1

    summary = []
    for (nick, castle), data in stats.items():
        success_rate = data["success"] / data["total"] * 100 if data["total"] > 0 else 0
        avg_time = round(sum(data["times"]) / len(data["times"]), 2) if data["times"] else "-"
        summary.append([
            nick,
            castle,
            data["total"],
            data["success"],
            data["fail"],
            f"{success_rate:.1f}%",
            avg_time
        ])

    return summary

def send_webhook(stats_table):
    headers = {"Content-Type": "application/json"}
    table = tabulate(stats_table, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="github", stralign="center", numalign="center")
    payload = {
        "content": f"```\n{table}\n```"
    }
    requests.post(WEBHOOK_URL, json=payload, headers=headers)

def main():
    print("[INFO] Inicjalizacja bazy...")
    conn = psycopg2.connect(**DB_CONFIG)
    init_db(conn)

    print("[INFO] Pobieranie logów...")
    files = get_ftp_log_files()
    print(f"[DEBUG] Liczba logów: {len(files)}")

    all_entries = []
    for file in files:
        content = download_log_file(file)
        for line in content.splitlines():
            if "LockpickingMinigame_C" in line:
                parsed = parse_log_line(line)
                if parsed:
                    all_entries.append(parsed)

    print(f"[DEBUG] Sparsowane wpisy lockpick: {len(all_entries)}")
    if not all_entries:
        print("[INFO] Brak danych lockpick.")
        return

    print("[INFO] Zapisywanie do bazy...")
    new = insert_new_entries(conn, all_entries)
    print(f"[INFO] Nowe wpisy zapisane: {new}")

    if new > 0:
        print("[INFO] Generowanie statystyk...")
        with conn.cursor() as cur:
            stats = get_statistics(cur)
        print("[INFO] Wysyłanie webhooka...")
        send_webhook(stats)
    else:
        print("[INFO] Brak nowych danych do wysłania.")

    conn.close()

# Serwer Flask do pingowania
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    main()
    app.run(host="0.0.0.0", port=3000)
