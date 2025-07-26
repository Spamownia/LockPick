import ftplib
import io
import re
import psycopg2
import requests
from collections import defaultdict
from datetime import datetime
from tabulate import tabulate
from flask import Flask

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# Konfiguracja webhooka Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Konfiguracja bazy danych PostgreSQL (Neon)
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Flask (na potrzeby pingowania)
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"
app.run(host="0.0.0.0", port=3000)

# Połączenie z bazą
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

# Pobierz listę plików z FTP (ręczne parsowanie `dir`)
def list_log_files(ftp):
    files = []

    def parse_line(line):
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            filename = parts[8]
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                files.append(filename)

    ftp.dir(parse_line)
    return files

# Pobierz i przetwórz wszystkie logi
def download_logs():
    print("[INFO] Pobieranie logów...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    filenames = list_log_files(ftp)
    logs = {}

    for name in filenames:
        with io.BytesIO() as bio:
            ftp.retrbinary(f"RETR {name}", bio.write)
            content = bio.getvalue().decode("utf-16-le", errors="ignore")
            logs[name] = content

    ftp.quit()
    return logs

# Przetwórz logi i zaktualizuj bazę
def process_logs(logs):
    pattern = re.compile(
        r"\[(?P<time>\d+\.\d+)s\] Player (?P<nick>.+?) tried to pick (?P<castle>\w+) lock: (?P<result>Success|Failed)",
        re.IGNORECASE,
    )

    conn = connect_db()
    cur = conn.cursor()

    # Tworzenie tabeli jeśli nie istnieje
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            castle TEXT,
            success_count INT,
            fail_count INT,
            total_time FLOAT,
            attempts INT,
            PRIMARY KEY (nick, castle)
        );
    """)
    conn.commit()

    stats = defaultdict(lambda: {"success": 0, "fail": 0, "time": 0.0})

    for content in logs.values():
        for match in pattern.finditer(content):
            nick = match["nick"]
            castle = match["castle"]
            result = match["result"].lower()
            try:
                time = float(match["time"])
            except ValueError:
                continue

            key = (nick, castle)
            stats[key]["time"] += time
            stats[key]["success" if result == "success" else "fail"] += 1

    # Aktualizacja bazy
    for (nick, castle), data in stats.items():
        cur.execute("""
            INSERT INTO lockpicking (nick, castle, success_count, fail_count, total_time, attempts)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (nick, castle)
            DO UPDATE SET
                success_count = lockpicking.success_count + EXCLUDED.success_count,
                fail_count = lockpicking.fail_count + EXCLUDED.fail_count,
                total_time = lockpicking.total_time + EXCLUDED.total_time,
                attempts = lockpicking.attempts + EXCLUDED.attempts;
        """, (
            nick, castle,
            data["success"],
            data["fail"],
            data["time"],
            data["success"] + data["fail"],
        ))
    conn.commit()

    # Pobierz wszystko do wysyłki
    cur.execute("SELECT nick, castle, attempts, success_count, fail_count, total_time FROM lockpicking")
    rows = cur.fetchall()
    conn.close()
    return rows

# Sformatuj i wyślij dane do Discorda
def send_to_discord(rows):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = []

    for row in rows:
        nick, castle, attempts, success, fail, total_time = row
        effectiveness = f"{(success / attempts * 100):.1f}%" if attempts > 0 else "0%"
        avg_time = f"{(total_time / attempts):.2f}s" if attempts > 0 else "-"
        table.append([nick, castle, str(attempts), str(success), str(fail), effectiveness, avg_time])

    # Ustawienie wyśrodkowania
    formatted = tabulate(table, headers, tablefmt="grid", colalign=("center",) * len(headers))

    requests.post(WEBHOOK_URL, json={"content": f"```\n{formatted}\n```"})

# Główna funkcja
def main():
    print("[INFO] Inicjalizacja bazy...")
    logs = download_logs()
    rows = process_logs(logs)
    send_to_discord(rows)

if __name__ == "__main__":
    main()
