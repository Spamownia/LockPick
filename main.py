# --- IMPORTY ---
import ftplib
import sqlite3
import threading
import time
import re
import io
import os
from flask import Flask

# --- KONFIGURACJA FTP I WEBHOOK ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- KONFIGURACJA BAZY DANYCH ---
DB_FILE = "lockpick.db"

# --- INICJALIZACJA BAZY ---
def init_db():
    print("[DEBUG] Inicjalizacja bazy danych...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            line TEXT UNIQUE
        )
    """)
    conn.commit()
    conn.close()
    print("[DEBUG] Baza danych zainicjowana.")

# --- POBIERANIE I PRZETWARZANIE LOGÓW ---
def download_logs_from_ftp():
    print("[DEBUG] Rozpoczynam pobieranie logów z FTP...")
    entries = []
    try:
        ftp = ftplib.FTP()
        print("[DEBUG] Łączenie z FTP...")
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        print("[DEBUG] Połączono z FTP")

        ftp.cwd('/SCUM/Saved/SaveFiles/Logs')
        print("[DEBUG] Zmieniono katalog na /SCUM/Saved/SaveFiles/Logs")
        files = ftp.nlst()
        print(f"[DEBUG] Pliki na serwerze: {files}")

        for filename in files:
            if re.match(r'gameplay_.*\.log', filename):
                print(f"[INFO] Pobieranie pliku: {filename}")
                r = io.BytesIO()
                ftp.retrbinary(f"RETR {filename}", r.write)
                r.seek(0)
                lines = r.read().decode('utf-8', errors='ignore').splitlines()

                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                for line in lines:
                    try:
                        c.execute("INSERT INTO logs (filename, line) VALUES (?, ?)", (filename, line))
                        entries.append(line)
                    except sqlite3.IntegrityError:
                        pass  # Linia już istnieje
                conn.commit()
                conn.close()
        ftp.quit()
        print(f"[DEBUG] Zakończono pobieranie logów, znaleziono {len(entries)} nowych wpisów.")
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania logów: {e}")
    return entries

# --- GŁÓWNA PĘTLA ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    while True:
        print("[DEBUG] Iteracja pętli głównej...")
        new_entries = download_logs_from_ftp()
        if new_entries:
            print(f"[INFO] Znaleziono {len(new_entries)} nowych wpisów.")
            # Tutaj dodaj wysyłanie do webhook lub dalsze przetwarzanie
        else:
            print("[INFO] Brak nowych wpisów.")
        time.sleep(60)

# --- FLASK ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=10000, use_reloader=False)
