import os
import re
import psycopg2
import requests
import threading
from flask import Flask
from datetime import datetime
from ftplib import FTP_TLS
from io import BytesIO, StringIO

app = Flask(__name__)

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

FTP_CONFIG = {
    "host": "176.57.174.10",
    "port": 50021,
    "user": "gpftp37275281717442833",
    "passwd": "LXNdGShY"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

lockpick_regex = re.compile(
    r"\[(?P<time>[^\]]+)\] \[(?P<level>.*?)\] \[(?P<category>.*?)\] \[(?P<context>.*?)\] \[(?P<msgid>.*?)\] "
    r"Lockpicking result for (?P<nick>.+?): (?P<result>SUCCESS|FAILURE), castle: (?P<zamek>.+?), time: (?P<czas>[0-9.]+)s"
)

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicks (
            id SERIAL PRIMARY KEY,
            nick TEXT,
            zamek TEXT,
            wynik TEXT,
            czas REAL,
            unikalny_id TEXT UNIQUE
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DEBUG] Inicjalizacja bazy danych...")

def parse_log_file(filename, content):
    new_entries = []
    for match in lockpick_regex.finditer(content):
        nick = match.group("nick").strip()
        zamek = match.group("zamek").strip()
        wynik = match.group("result").strip()
        czas = float(match.group("czas"))
        unikalny_id = f"{filename}-{match.start()}"
        new_entries.append((nick, zamek, wynik, czas, unikalny_id))
    return new_entries

def process_logs():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    ftps = FTP_TLS()
    ftps.connect(FTP_CONFIG["host"], FTP_CONFIG["port"])
    ftps.login(FTP_CONFIG["user"], FTP_CONFIG["passwd"])
    ftps.prot_p()
    ftps.cwd(LOG_DIR)

    listing = []
    ftps.retrlines('LIST', listing.append)

    log_filenames = [line.split()[-1] for line in listing if line.split()[-1].startswith("gameplay_") and line.split()[-1].endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_filenames)} plików logów gameplay_*.log")

    all_new_entries = []

    for filename in log_filenames:
        print(f"[DEBUG] Przetwarzanie pliku: {filename}")
        bio = BytesIO()
        try:
            ftps.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode('utf-16-le', errors='ignore')
            entries = parse_log_file(filename, content)
            print(f"[DEBUG] Znaleziono {len(entries)} wpisów w pliku {filename}")
            for entry in entries:
                try:
                    cur.execute(
                        "INSERT INTO lockpicks (nick, zamek, wynik, czas, unikalny_id) VALUES (%s, %s, %s, %s, %s)",
                        entry
                    )
                    all_new_entries.append(entry)
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
                    continue
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR] Błąd podczas zapisu do bazy: {e}")
                else:
                    conn.commit()
        except Exception as e:
            print(f"[ERROR] Błąd przy pobieraniu pliku {filename}: {e}")
        finally:
            bio.close()

    ftps.quit()
    cur.close()
    conn.close()
    print(f"[DEBUG] Zapisano {len(all_new_entries)} nowych wpisów do bazy.")
    return all_new_entries

def fetch_stats():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, zamek,
            COUNT(*) AS proby,
            COUNT(*) FILTER (WHERE wynik = 'SUCCESS') AS udane,
            COUNT(*) FILTER (WHERE wynik = 'FAILURE') AS nieudane,
            ROUND(100.0 * COUNT(*) FILTER (WHERE wynik = 'SUCCESS') / NULLIF(COUNT(*), 0), 2) AS skutecznosc,
            ROUND(AVG(czas), 2) AS sredni_czas
        FROM lockpicks
        GROUP BY nick, zamek
        ORDER BY skutecznosc DESC NULLS LAST, sredni_czas ASC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def format_table(rows):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [len(h) for h in headers]

    for row in rows:
        for i, val in enumerate(row):
            val_str = f"{val}" if not isinstance(val, float) else f"{val:.2f}"
            col_widths[i] = max(col_widths[i], len(val_str))

    lines = []

    def format_row(row_items):
        return " | ".join(str(val).center(width) for val, width in zip(row_items, col_widths))

    lines.append(format_row(headers))
    lines.append("-+-".join("-" * width for width in col_widths))

    for row in rows:
        formatted = []
        for val in row:
            if isinstance(val, float):
                formatted.append(f"{val:.2f}")
            else:
                formatted.append(str(val))
        lines.append(format_row(formatted))

    return "```\n" + "\n".join(lines) + "\n```"

def send_to_discord(content):
    response = requests.post(WEBHOOK_URL, json={"content": content})
    print(f"[DEBUG] Webhook status: {response.status_code}")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    new_entries = process_logs()
    if new_entries:
        stats = fetch_stats()
        if stats:
            tabela = format_table(stats)
            send_to_discord(tabela)
    else:
        print("[DEBUG] Brak nowych wpisów do wysłania.")

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    threading.Thread(target=main_loop).start()
    app.run(host='0.0.0.0', port=3000)
