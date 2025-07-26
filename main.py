import os
import re
import time
import ftplib
import psycopg2
import threading
import requests
from io import BytesIO
from flask import Flask

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_HOST = "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech"
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASS = "npg_dRU1YCtxbh6v"
DB_PORT = 5432
DB_SSL = "require"

app = Flask(__name__)
processed_entries = set()


# --- BAZA DANYCH ---
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        sslmode=DB_SSL
    )


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicking (
                    nick TEXT,
                    castle TEXT,
                    success BOOLEAN,
                    time FLOAT
                );
            """)
            conn.commit()


def insert_entry(nick, castle, success, time_val):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO lockpicking (nick, castle, success, time)
                VALUES (%s, %s, %s, %s);
            """, (nick, castle, success, time_val))
            conn.commit()


# --- PRZETWARZANIE ---
def parse_log(content):
    lines = content.splitlines()
    found_new = False

    pattern = re.compile(
        r'(?P<date>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}).*?\[LockpickingComponent\].*?Lockpicking (?P<result>SUCCESS|FAILED) \[Nickname: (?P<nick>[^\]]+)\] \[Time: (?P<time>\d+,\d+|\d+\.\d+|\d+)\] \[Lock Type: (?P<castle>[^\]]+)\]'
    )

    for line in lines:
        match = pattern.search(line)
        if match:
            entry_id = hash(line)
            if entry_id in processed_entries:
                continue
            processed_entries.add(entry_id)
            found_new = True

            nick = match.group("nick")
            castle = match.group("castle")
            result = match.group("result")
            success = result == "SUCCESS"

            time_str = match.group("time")
            time_val = float(time_str.replace(",", ".").replace(".", "", time_str.count(".") > 1))

            insert_entry(nick, castle, success, time_val)

    return found_new


def generate_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, castle,
                       COUNT(*) as total,
                       COUNT(*) FILTER (WHERE success) as success_count,
                       COUNT(*) FILTER (WHERE NOT success) as fail_count,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE success) / NULLIF(COUNT(*), 0), 1) as effectiveness,
                       ROUND(AVG(time), 2) as avg_time
                FROM lockpicking
                GROUP BY nick, castle
                ORDER BY effectiveness DESC, avg_time ASC;
            """)
            rows = cur.fetchall()

    if not rows:
        return None

    headers = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    # Długość kolumn
    col_widths = [max(len(str(row[i])) for row in rows + [headers]) for i in range(len(headers))]

    table = "```"
    table += "\n" + " | ".join(h.center(col_widths[i]) for i, h in enumerate(headers))
    table += "\n" + "-+-".join("-" * w for w in col_widths)
    for row in rows:
        table += "\n" + " | ".join(str(cell).center(col_widths[i]) for i, cell in enumerate(row))
    table += "\n```"
    return table


# --- LOGIKA GŁÓWNA ---
def main_loop():
    init_db()

    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_DIR)

            files = ftp.nlst("gameplay_*.log")
            if not files:
                print("[INFO] Brak plików gameplay_*.log na FTP.")
                return

            files.sort()
            found_new = False

            for filename in files:
                bio = BytesIO()
                ftp.retrbinary(f"RETR {filename}", bio.write)
                bio.seek(0)
                content = bio.read().decode("utf-16-le", errors="ignore")
                if parse_log(content):
                    found_new = True

            if found_new:
                print("[INFO] Wykryto nowe wpisy.")
                table = generate_table()
                if table:
                    requests.post(WEBHOOK_URL, json={"content": table})
            else:
                print("[INFO] Brak nowych wpisów w logach.")

    except Exception as e:
        print(f"[ERROR] {e}")


# --- PING UPTIME ROBOT ---
@app.route("/")
def index():
    return "Alive"


# --- URUCHOMIENIE ---
if __name__ == "__main__":
    threading.Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
