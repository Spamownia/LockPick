import os
import re
import psycopg2
import ftplib
import io
import requests
from collections import defaultdict
from flask import Flask

# ---------------------------------------
# KONFIGURACJA
# ---------------------------------------

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# ---------------------------------------
# INICJALIZACJA BAZY
# ---------------------------------------

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            castle TEXT,
            success BOOLEAN,
            duration FLOAT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# ---------------------------------------
# POBIERANIE LOGÓW Z FTP
# ---------------------------------------

def download_logs():
    logs = []
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(LOG_DIR)
        filenames = ftp.nlst("gameplay_*.log")
        for filename in filenames:
            with io.BytesIO() as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
                content = f.getvalue().decode("utf-16-le", errors="ignore")
                logs.append(content)
    return logs

# ---------------------------------------
# PARSOWANIE LOGÓW
# ---------------------------------------

LOG_PATTERN = re.compile(
    r'(?P<time>\d+\.\d+).*?LockpickingComponent.*?CharacterName: (?P<nick>.*?) .*?on castle (?P<castle>.*?)\. Result: (?P<result>SUCCESS|FAILED).*?in (?P<duration>\d+\.\d+)s',
    re.DOTALL
)

def parse_logs(logs):
    parsed = []
    for log in logs:
        for match in LOG_PATTERN.finditer(log):
            parsed.append({
                "nick": match.group("nick"),
                "castle": match.group("castle"),
                "success": match.group("result") == "SUCCESS",
                "duration": float(match.group("duration"))
            })
    return parsed

# ---------------------------------------
# ZAPIS DO BAZY
# ---------------------------------------

def insert_to_db(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for e in entries:
        cur.execute("""
            INSERT INTO lockpick_stats (nick, castle, success, duration)
            VALUES (%s, %s, %s, %s);
        """, (e["nick"], e["castle"], e["success"], e["duration"]))
    conn.commit()
    cur.close()
    conn.close()

# ---------------------------------------
# GENEROWANIE TABELI
# ---------------------------------------

def build_stats_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT nick, castle,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE success) as success_count,
            COUNT(*) FILTER (WHERE NOT success) as fail_count,
            ROUND(100.0 * COUNT(*) FILTER (WHERE success)::NUMERIC / COUNT(*), 1) as effectiveness,
            ROUND(AVG(duration), 2) as avg_time
        FROM lockpick_stats
        GROUP BY nick, castle
        ORDER BY effectiveness DESC, total DESC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [max(len(str(r[i])) for r in rows + [headers]) for i in range(len(headers))]

    def center(text, width):
        return str(text).center(width)

    lines = [" | ".join([center(h, col_widths[i]) for i, h in enumerate(headers)])]
    lines.append("-+-".join(["-" * w for w in col_widths]))
    for row in rows:
        lines.append(" | ".join([center(str(cell), col_widths[i]) for i, cell in enumerate(row)]))

    return "```\n" + "\n".join(lines) + "\n```"

# ---------------------------------------
# WYSYŁKA NA WEBHOOK
# ---------------------------------------

def send_to_webhook(message):
    requests.post(WEBHOOK_URL, json={"content": message})

# ---------------------------------------
# MAIN
# ---------------------------------------

def main():
    print("[INFO] Inicjalizacja bazy...")
    init_db()

    print("[INFO] Pobieranie logów...")
    logs = download_logs()
    print(f"[INFO] Liczba logów: {len(logs)}")

    print("[INFO] Parsowanie danych...")
    parsed = parse_logs(logs)
    print(f"[INFO] Wpisów do bazy: {len(parsed)}")

    print("[INFO] Wstawianie do bazy...")
    insert_to_db(parsed)

    print("[INFO] Generowanie tabeli...")
    tabela = build_stats_table()

    print("[INFO] Wysyłanie na webhook...")
    send_to_webhook(tabela)
    print("[OK] Gotowe")

# ---------------------------------------
# Flask (ping render)
# ---------------------------------------

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    main()
    app.run(host="0.0.0.0", port=3000)
