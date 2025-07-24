# --- main.py ---

import time
import threading
import sqlite3
import requests
from ftplib import FTP
from flask import Flask

# --- KONFIGURACJA FTP I WEBHOOK ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- INICJALIZACJA BAZY SQLITE ---
DB_NAME = "logs.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS logs
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  filename TEXT,
                  line TEXT)''')
    conn.commit()
    conn.close()

# --- PARSER LINII LOGU (PRZYKŁADOWY) ---
def parse_line(line):
    # 🔧 dostosuj do formatu Twoich logów
    if line.strip() == "":
        return None
    return line.strip()

# --- POBIERANIE LOGÓW Z FTP ---
def download_logs_from_ftp():
    print("[DEBUG] Łączenie z FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    files = []

    # Próbujemy MLSD zamiast NLST
    try:
        ftp.retrlines('MLSD', files.append)
        log_files = [line.split(";")[-1].strip() for line in files if line.endswith(".log")]
    except:
        print("[ERROR] MLSD również nie działa, podaj pełne ścieżki plików do pobrania.")
        ftp.quit()
        return []

    entries = []
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    for filename in log_files:
        print(f"[INFO] Pobieram {filename}")
        lines = []
        ftp.retrlines(f"RETR {filename}", lines.append)
        for line in lines:
            parsed = parse_line(line)
            if parsed:
                # Zapis do DB jeśli nie istnieje
                c.execute("SELECT COUNT(*) FROM logs WHERE filename=? AND line=?", (filename, parsed))
                if c.fetchone()[0] == 0:
                    c.execute("INSERT INTO logs (filename, line) VALUES (?, ?)", (filename, parsed))
                    entries.append(parsed)

    conn.commit()
    conn.close()
    ftp.quit()
    return entries

# --- WYSYŁKA NA WEBHOOK ---
def send_to_webhook(entries):
    if not entries:
        print("[INFO] Brak nowych wpisów do wysłania.")
        return
    content = "\n".join(entries)
    data = {"content": f"Nowe logi:\n{content}"}
    response = requests.post(WEBHOOK_URL, json=data)
    print(f"[INFO] Webhook status: {response.status_code}")

# --- PĘTLA GŁÓWNA ---
def main_loop():
    while True:
        entries = download_logs_from_ftp()
        send_to_webhook(entries)
        time.sleep(60)

# --- URUCHOMIENIE APLIKACJI FLASK I WĄTKU ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Lockpick Stats App Running."

if __name__ == "__main__":
    init_db()
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
