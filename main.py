# --- AUTOMATYCZNA INSTALACJA BIBLIOTEK ---
import subprocess
import sys

for pkg in ["requests", "flask"]:
    try:
        __import__(pkg)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

import sqlite3
import time
import threading
import requests
from ftplib import FTP
from flask import Flask

# --- KONFIGURACJA FTP I WEBHOOK ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_PATH = "logs.db"

# --- INICJALIZACJA BAZY SQLITE ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS logs
                 (filename TEXT, line TEXT)''')
    conn.commit()
    conn.close()
    print("[DEBUG] Baza danych SQLite zainicjowana:", DB_PATH)

# --- POBRANIE LISTY LOGÓW Z FTP ---
def download_logs_from_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print("[DEBUG] Połączono z FTP")

    ftp.cwd('/SCUM/Saved/SaveFiles/Logs')
    print("[DEBUG] Obecny katalog FTP:", ftp.pwd())

    files = ftp.nlst()
    print("[DEBUG] Wszystkie pliki w folderze Logs:", files)

    log_files = [f for f in files if f.startswith('gameplay_') and f.endswith('.log')]
    print("[DEBUG] Filtrowane gameplay_*.log:", log_files)

    entries = []

    for filename in log_files:
        print(f"[INFO] Pobieram plik: {filename}")
        lines = []
        ftp.retrlines(f"RETR {filename}", lines.append)
        for line in lines:
            entries.append( (filename, line) )

    ftp.quit()
    return entries

# --- ZAPIS DO BAZY SQLITE ---
def save_to_sqlite(entries):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    new_count = 0

    for filename, line in entries:
        c.execute("SELECT 1 FROM logs WHERE filename=? AND line=?", (filename, line))
        if not c.fetchone():
            c.execute("INSERT INTO logs VALUES (?, ?)", (filename, line))
            new_count += 1

    conn.commit()
    conn.close()
    print(f"[INFO] Zapisano {new_count} nowych linii do bazy danych.")
    return new_count

# --- WYŚLIJ NA WEBHOOK ---
def send_webhook(message):
    try:
        r = requests.post(WEBHOOK_URL, json={"content": message})
        if r.status_code == 204:
            print("[INFO] Webhook wysłany poprawnie.")
        else:
            print(f"[WARNING] Błąd wysyłania webhook: {r.status_code} {r.text}")
    except Exception as e:
        print("[ERROR] Wyjątek podczas wysyłki webhook:", e)

# --- PĘTLA GŁÓWNA ---
def main_loop():
    init_db()
    while True:
        print("[DEBUG] Rozpoczynam cykl pętli...")
        try:
            entries = download_logs_from_ftp()
            new_lines = save_to_sqlite(entries)
            if new_lines > 0:
                send_webhook(f"✅ Dodano {new_lines} nowych linii do bazy.")
            else:
                print("[INFO] Brak nowych wpisów.")
        except Exception as e:
            print("[ERROR] Błąd w pętli głównej:", e)
        time.sleep(60)

# --- FLASK KEEPALIVE ---
app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

if __name__ == "__main__":
    threading.Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=10000)
