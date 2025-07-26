import ftplib
import os
import re
import psycopg2
import requests
import time
import io
from flask import Flask

# --- KONFIGURACJA ---
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

# --- WZORZEC DANYCH Z LOGA ---
LOG_PATTERN = re.compile(
    r"User: (?P<nick>.*?) \(\d+, \d+\)\. Success: (?P<result>Yes|No)\. Elapsed time: (?P<time>\d+\.\d+)\. .*?Lock type: (?P<lock_type>\w+)",
    re.MULTILINE
)

# --- FLASK KEEP-ALIVE ---
app = Flask(__name__)
@app.route("/")
def index():
    return "Alive"
def run_flask():
    app.run(host="0.0.0.0", port=3000)

# --- FUNKCJE BAZY ---
def init_db(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            castle TEXT,
            result TEXT,
            time FLOAT
        )
    """)
    # Upewnij się, że kolumna castle przyjmuje Lock Type
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'lockpick_stats'")
    existing_columns = [row[0] for row in cur.fetchall()]
    if 'castle' not in existing_columns:
        cur.execute("ALTER TABLE lockpick_stats ADD COLUMN castle TEXT")
    if 'result' not in existing_columns:
        print("[INFO] Dodawanie brakującej kolumny 'result'...")
        cur.execute("ALTER TABLE lockpick_stats ADD COLUMN result TEXT")

def entry_exists(cur, entry):
    cur.execute("""
        SELECT 1 FROM lockpick_stats
        WHERE nick = %s AND castle = %s AND result = %s AND time = %s
        """, (entry["nick"], entry["castle"], entry["result"], float(entry["time"])))
    return cur.fetchone() is not None

def save_entries(cur, conn, entries):
    new_count = 0
    for entry in entries:
        if not entry_exists(cur, entry):
            cur.execute("""
                INSERT INTO lockpick_stats (nick, castle, result, time)
                VALUES (%s, %s, %s, %s)
            """, (entry["nick"], entry["castle"], entry["result"], float(entry["time"])))
            new_count += 1
    conn.commit()
    return new_count

# --- FUNKCJE LOGÓW ---
def parse_logs(logs):
    parsed = []
    for content in logs:
        for match in LOG_PATTERN.finditer(content):
            try:
                parsed.append({
                    "nick": match.group("nick"),
                    "castle": match.group("lock_type"),
                    "result": "Success" if match.group("result") == "Yes" else "Fail",
                    "time": match.group("time").rstrip(".")  # Usuwa końcową kropkę jeśli wystąpi
                })
            except Exception:
                continue
    return parsed

def fetch_logs():
    print("[INFO] Połączono z FTP.")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_PATH)
    files = ftp.nlst()
    log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    logs = []
    for file_name in log_files:
        with io.BytesIO() as bio:
            ftp.retrbinary(f"RETR {file_name}", bio.write)
            bio.seek(0)
            content = bio.read().decode("utf-16-le", errors="ignore")
            logs.append(content)
    ftp.quit()
    return logs

# --- STATYSTYKI I WEBHOOK ---
def get_statistics(cur):
    cur.execute("SELECT nick, castle, result, time FROM lockpick_stats")
    rows = cur.fetchall()
    stats = {}
    for nick, castle, result, time_val in rows:
        key = (nick, castle)
        if key not in stats:
            stats[key] = {
                "total": 0,
                "success": 0,
                "fail": 0,
                "times": []
            }
        stats[key]["total"] += 1
        if result == "Success":
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1
        stats[key]["times"].append(time_val)
    return stats

def format_table(stats):
    lines = []
    header = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join([" :--: " for _ in header]) + "|")
    for (nick, castle), data in stats.items():
        total = data["total"]
        success = data["success"]
        fail = data["fail"]
        accuracy = f"{(success / total * 100):.1f}%" if total else "0%"
        avg_time = f"{(sum(data['times']) / total):.2f}" if total else "-"
        row = [nick, castle, str(total), str(success), str(fail), accuracy, avg_time]
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)

def send_to_webhook(stats):
    if not stats:
        print("[INFO] Brak nowych danych – webhook nie wysłany.")
        return
    content = format_table(stats)
    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{content}\n```"})
    if response.status_code == 204:
        print("[INFO] Webhook wysłany.")
    else:
        print("[ERROR] Błąd wysyłania webhooka:", response.status_code, response.text)

# --- MAIN ---
def main():
    print("[INFO] Pobieranie logów...")
    logs = fetch_logs()
    print(f"[DEBUG] Liczba logów: {len(logs)}")
    print("[INFO] Parsowanie danych...")
    all_entries = parse_logs(logs)
    print(f"[DEBUG] Sparsowane wpisy lockpick: {len(all_entries)}")

    if not all_entries:
        print("[INFO] Brak danych lockpick.")
        return

    print("[INFO] Inicjalizacja bazy...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    init_db(cur)

    print("[INFO] Zapisywanie do bazy...")
    new_count = save_entries(cur, conn, all_entries)
    print(f"[DEBUG] Nowe wpisy: {new_count}")
    if new_count == 0:
        print("[INFO] Brak nowych danych – webhook nie wysłany.")
        return

    print("[INFO] Generowanie statystyk...")
    stats = get_statistics(cur)
    send_to_webhook(stats)
    cur.close()
    conn.close()

if __name__ == "__main__":
    from threading import Thread
    Thread(target=run_flask).start()
    time.sleep(2)
    main()
