import ftplib
import os
import re
import psycopg2
import requests
from io import BytesIO
from datetime import datetime
from collections import defaultdict
from flask import Flask

# --- KONFIGURACJE ---
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

LOG_PATTERN = re.compile(
    r"\[\w+Minigame\].*?User: (?P<nick>.*?) \(\d+, \d+\)\. Success: (?P<result>\w+).*?Elapsed time: (?P<time>[\d.]+)\.*.*?Target object: .*?_(?P<castle>\w+)_.*?"
)

# --- FLASK KEEPALIVE ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- FUNKCJE ---

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    print("[INFO] Połączono z FTP.")
    return ftp

def get_ftp_log_files():
    ftp = connect_ftp()
    files = []
    ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]) if "gameplay_" in line else None)
    return sorted(set(f for f in files if f.startswith("gameplay_") and f.endswith(".log")))

def parse_logs_from_ftp():
    ftp = connect_ftp()
    files = get_ftp_log_files()
    print(f"[DEBUG] Liczba logów: {len(files)}")
    all_entries = []

    for filename in files:
        bio = BytesIO()
        try:
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode("utf-16-le", errors="ignore")
            for line in content.splitlines():
                match = LOG_PATTERN.search(line)
                if match:
                    nick = match.group("nick").strip()
                    castle = match.group("castle").strip()
                    result = match.group("result").strip()
                    time_str = match.group("time").strip().rstrip(".")
                    try:
                        time_val = float(time_str)
                    except ValueError:
                        continue
                    all_entries.append({
                        "nick": nick,
                        "castle": castle,
                        "result": result,
                        "time": time_val
                    })
        except Exception as e:
            print(f"[ERROR] Błąd przy pliku {filename}: {e}")
    ftp.quit()
    return all_entries

def init_db():
    print("[INFO] Inicjalizacja bazy...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_history (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            castle TEXT,
            result TEXT,
            time FLOAT,
            timestamp TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()
    return conn, cur

def entry_exists(cur, entry):
    cur.execute("""
        SELECT 1 FROM lockpick_history 
        WHERE nick=%s AND castle=%s AND result=%s AND time=%s
        LIMIT 1
    """, (entry["nick"], entry["castle"], entry["result"], float(entry["time"])))
    return cur.fetchone() is not None

def save_entries(cur, conn, entries):
    new_count = 0
    for entry in entries:
        if not entry_exists(cur, entry):
            cur.execute("""
                INSERT INTO lockpick_history (nick, castle, result, time) 
                VALUES (%s, %s, %s, %s)
            """, (entry["nick"], entry["castle"], entry["result"], float(entry["time"])))
            new_count += 1
    conn.commit()
    return new_count

def calculate_stats(cur):
    cur.execute("SELECT nick, castle, result, time FROM lockpick_history")
    stats = defaultdict(lambda: defaultdict(list))
    for nick, castle, result, time in cur.fetchall():
        stats[(nick, castle)]["all"].append(time)
        if result.lower() == "yes":
            stats[(nick, castle)]["success"].append(time)
        else:
            stats[(nick, castle)]["fail"].append(time)

    results = []
    for (nick, castle), data in stats.items():
        total = len(data["all"])
        success = len(data["success"])
        fail = len(data["fail"])
        effectiveness = f"{(success / total * 100):.1f}%" if total else "0%"
        avg_time = f"{(sum(data['success']) / success):.2f}s" if success else "-"
        results.append([nick, castle, str(total), str(success), str(fail), effectiveness, avg_time])
    return results

def format_table(rows):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*([headers] + rows))]
    table = "```"
    table += "\n" + " | ".join(str(h).center(w) for h, w in zip(headers, col_widths))
    table += "\n" + "-+-".join("-" * w for w in col_widths)
    for row in rows:
        table += "\n" + " | ".join(str(c).center(w) for c, w in zip(row, col_widths))
    table += "```"
    return table

def send_to_discord(message):
    requests.post(WEBHOOK_URL, json={"content": message})

def main():
    print("[INFO] Pobieranie logów...")
    all_entries = parse_logs_from_ftp()
    print(f"[INFO] Parsowanie danych...")
    print(f"[DEBUG] Sparsowane wpisy lockpick: {len(all_entries)}")

    conn, cur = init_db()
    print("[INFO] Zapisywanie do bazy...")
    new_count = save_entries(cur, conn, all_entries)
    print(f"[DEBUG] Nowe wpisy: {new_count}")

    if new_count > 0:
        rows = calculate_stats(cur)
        table = format_table(rows)
        send_to_discord(table)
        print("[INFO] Webhook wysłany.")
    else:
        print("[INFO] Brak nowych danych – webhook nie wysłany.")

    cur.close()
    conn.close()

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    from threading import Thread
    Thread(target=lambda: app.run(host="0.0.0.0", port=3000)).start()
    main()
