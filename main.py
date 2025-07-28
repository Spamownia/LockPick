import os
import re
import time
import ftplib
import pandas as pd
import psycopg2
from tabulate import tabulate
from flask import Flask
from io import BytesIO

# Konfiguracja połączenia z FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# Konfiguracja Webhooka
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Konfiguracja bazy danych PostgreSQL
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Flask keep-alive
app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def get_ftp_log_files():
    print("[DEBUG] Nawiązywanie połączenia FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)
    files = ftp.nlst()
    log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików logów.")
    return ftp, log_files

def parse_log_content(content):
    decoded = content.decode("utf-16-le", errors="ignore")
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>\w+).*?Type: (?P<lock>.+?)\..*?Success: (?P<success>Yes|No).*?Elapsed time: (?P<time>[\d.]+)",
        re.DOTALL
    )
    entries = []
    for match in pattern.finditer(decoded):
        entries.append({
            "nick": match.group("nick"),
            "lock": match.group("lock"),
            "success": match.group("success") == "Yes",
            "time": float(match.group("time"))
        })
    print(f"[DEBUG] Rozpoznano {len(entries)} wpisów lockpick.")
    return entries

def save_entries_to_db(entries):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            lock TEXT,
            success BOOLEAN,
            time FLOAT
        )
    """)
    for e in entries:
        cur.execute("INSERT INTO lockpick_stats (nick, lock, success, time) VALUES (%s, %s, %s, %s)",
                    (e["nick"], e["lock"], e["success"], e["time"]))
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Zapisano {len(entries)} wpisów do bazy.")

def generate_stats_table():
    conn = connect_db()
    df = pd.read_sql_query("SELECT * FROM lockpick_stats", conn)
    conn.close()

    if df.empty:
        return None

    grouped = df.groupby(["nick", "lock"]).agg(
        Wszystkie=pd.NamedAgg(column="success", aggfunc="count"),
        Udane=pd.NamedAgg(column="success", aggfunc="sum"),
        Nieudane=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        Skuteczność=pd.NamedAgg(column="success", aggfunc=lambda x: f"{(x.sum() / len(x) * 100):.1f}%"),
        Średni_czas=pd.NamedAgg(column="time", aggfunc=lambda x: f"{x.mean():.2f}s")
    ).reset_index()

    grouped.columns = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = tabulate(grouped, headers="keys", tablefmt="github", stralign="center", numalign="center")
    return table

def send_webhook_message(table):
    if table is None:
        print("[DEBUG] Brak danych do wysłania.")
        return
    payload = {"content": f"```\n{table}\n```"}
    import requests
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Wysłano dane do webhooka: {response.status_code}")

def process_logs():
    ftp, log_files = get_ftp_log_files()
    processed = 0
    for fname in log_files:
        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {fname}", bio.write)
            bio.seek(0)
            content = bio.read()
            entries = parse_log_content(content)
            if entries:
                save_entries_to_db(entries)
                processed += 1
    ftp.quit()
    print(f"[DEBUG] Przetworzono {processed} plików logów.")
    return processed > 0

def main_loop():
    print("[DEBUG] Start programu")
    if process_logs():
        table = generate_stats_table()
        send_webhook_message(table)
    else:
        print("[DEBUG] Brak nowych danych.")

if __name__ == "__main__":
    from threading import Thread
    t = Thread(target=lambda: app.run(host="0.0.0.0", port=3000))
    t.start()

    while True:
        try:
            main_loop()
        except Exception as e:
            print(f"[ERROR] Wystąpił błąd: {e}")
        time.sleep(60)
