import os import re import time import ftplib import threading import sqlite3 import chardet from datetime import datetime from flask import Flask

DB_FILE = "logs.db" FTP_HOST = os.getenv("FTP_HOST") FTP_USER = os.getenv("FTP_USER") FTP_PASS = os.getenv("FTP_PASS") FTP_DIR = os.getenv("FTP_DIR", "/") FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "300"))  # seconds

LOG_PATTERN = re.compile(r"^gameplay_(\d{14})\.log$") ENTRY_PATTERN = re.compile( r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) (?P<message>.+)$", re.MULTILINE )

app = Flask(name)

=== DATABASE ===

def init_db(): print("[INFO] Inicjalizacja bazy danych...") conn = sqlite3.connect(DB_FILE) c = conn.cursor() c.execute(''' CREATE TABLE IF NOT EXISTS logs ( id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT, timestamp TEXT, message TEXT ) ''') c.execute(''' CREATE TABLE IF NOT EXISTS processed_files ( filename TEXT PRIMARY KEY ) ''') conn.commit() conn.close()

=== FTP ===

def list_log_files(ftp): files = [] ftp.retrlines("MLSD" if "mlsd" in ftp.sendcmd("FEAT").lower() else "LIST", lambda line: files.append(line)) names = [] for line in files: parts = line.split() name = parts[-1] if LOG_PATTERN.match(name): names.append(name) return names

def fetch_log_files(): print("[DEBUG] Start fetch_log_files()") conn = sqlite3.connect(DB_FILE) c = conn.cursor()

ftp = ftplib.FTP(FTP_HOST)
ftp.login(FTP_USER, FTP_PASS)
ftp.cwd(FTP_DIR)

try:
    all_files = list_log_files(ftp)
    print(f"[DEBUG] Wszystkie logi na FTP: {all_files}")
    c.execute("SELECT filename FROM processed_files")
    processed = set(row[0] for row in c.fetchall())
    new_files = [f for f in all_files if f not in processed]

    for filename in sorted(new_files):
        print(f"[DEBUG] Przetwarzanie pliku: {filename}")
        try:
            lines = []
            ftp.retrbinary(f"RETR {filename}", lines.append)
            content_bytes = b"".join(lines)

            detected = chardet.detect(content_bytes)
            encoding = detected["encoding"] or "utf-8"
            content = content_bytes.decode(encoding, errors="replace")

            entries = ENTRY_PATTERN.findall(content)
            print(f"[DEBUG] Wyodrębniono {len(entries)} wpisów z logu")
            for timestamp, message in entries:
                c.execute("INSERT INTO logs (filename, timestamp, message) VALUES (?, ?, ?)",
                          (filename, timestamp, message))
            c.execute("INSERT INTO processed_files (filename) VALUES (?)", (filename,))
            conn.commit()
        except Exception as e:
            print(f"[ERROR] Błąd przetwarzania {filename}: {e}")

finally:
    ftp.quit()
    conn.close()

def main_loop(): print("[DEBUG] Start main_loop") while True: try: print("[DEBUG] Sprawdzam nowe logi...") fetch_log_files() except Exception as e: print(f"[ERROR] Błąd w głównej pętli: {e}") time.sleep(FETCH_INTERVAL)

@app.route("/") def index(): return "<h1>Log Processor is Running</h1>"

if name == "main": init_db() thread = threading.Thread(target=main_loop, daemon=True) thread.start() app.run(host="0.0.0.0", port=3000)

