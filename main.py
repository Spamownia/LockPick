import os
import re
import io
import time
import hashlib
import threading
import psycopg2
import requests
from flask import Flask
from ftplib import FTP

# Konfiguracja połączenia z FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# Konfiguracja bazy danych PostgreSQL
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)

def init_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gameplay_logs (
                    hash TEXT PRIMARY KEY,
                    nick TEXT,
                    zamek TEXT,
                    wynik TEXT,
                    czas REAL
                );
            """)
            conn.commit()
    print("[INFO] Inicjalizacja bazy danych...")

def hash_line(line):
    return hashlib.md5(line.encode("utf-16le")).hexdigest()

def parse_log_content(content):
    pattern = re.compile(r"\[(.*?)\] \[Lockpicking\] (.*?) attempted to pick (.*?) lock: (SUCCESS|FAIL) \((\d+\.\d+)s\)")
    results = []
    for match in pattern.finditer(content):
        nick = match.group(2).strip()
        zamek = match.group(3).strip()
        wynik = match.group(4).strip()
        czas = float(match.group(5))
        linia = match.group(0)
        hash_val = hash_line(linia)
        results.append((hash_val, nick, zamek, wynik, czas))
    return results

def insert_entries(entries):
    new = 0
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for entry in entries:
                try:
                    cur.execute("""
                        INSERT INTO gameplay_logs (hash, nick, zamek, wynik, czas)
                        VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
                    """, entry)
                    if cur.rowcount > 0:
                        new += 1
            conn.commit()
    print(f"[INFO] Nowe wpisy dodane: {new}")
    return new > 0

def fetch_all_files_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    files = []
    ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
    gameplay_logs = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[INFO] Znaleziono {len(gameplay_logs)} plików gameplay_*.log")
    contents = []
    for fname in gameplay_logs:
        stream = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {fname}", stream.write)
            stream.seek(0)
            text = stream.read().decode("utf-16le")
            contents.append(text)
        except Exception as e:
            print(f"[WARN] Błąd podczas pobierania pliku {fname}: {e}")
    ftp.quit()
    return contents

def generate_report():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nick, zamek, wynik, czas FROM gameplay_logs;")
            rows = cur.fetchall()

    stats = {}
    for nick, zamek, wynik, czas in rows:
        key = (nick, zamek)
        if key not in stats:
            stats[key] = {"total": 0, "success": 0, "fail": 0, "czasy": []}
        stats[key]["total"] += 1
        if wynik == "SUCCESS":
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1
        stats[key]["czasy"].append(czas)

    lines = []
    for (nick, zamek), s in stats.items():
        skutecznosc = round(s["success"] / s["total"] * 100, 2) if s["total"] else 0
        sr_czas = round(sum(s["czasy"]) / len(s["czasy"]), 2) if s["czasy"] else 0
        lines.append([nick, zamek, str(s["total"]), str(s["success"]), str(s["fail"]), f"{skutecznosc}%", f"{sr_czas}s"])

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    widths = [max(len(row[i]) for row in lines + [headers]) for i in range(len(headers))]

    table = "```" + "\n"
    table += " | ".join(f"{headers[i].center(widths[i])}" for i in range(len(headers))) + "\n"
    table += "-+-".join("-" * widths[i] for i in range(len(headers))) + "\n"
    for row in lines:
        table += " | ".join(f"{row[i].center(widths[i])}" for i in range(len(row))) + "\n"
    table += "```"

    requests.post(WEBHOOK_URL, json={"content": table})
    print("[INFO] Wysłano statystyki na Discorda.")

def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        all_texts = fetch_all_files_ftp()
        all_entries = []
        for content in all_texts:
            parsed = parse_log_content(content)
            all_entries.extend(parsed)
        if insert_entries(all_entries):
            generate_report()
        else:
            print("[INFO] Brak nowych zdarzeń.")
        time.sleep(60)

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
