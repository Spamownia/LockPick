# --- AUTOMATYCZNA INSTALACJA WYMAGANYCH BIBLIOTEK ---
import subprocess
import sys
import re
import threading
import time
import sqlite3
import requests
from ftplib import FTP

for pkg in ["requests"]:
    try:
        __import__(pkg)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

# --- KONFIGURACJA FTP I WEBHOOK ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- INICJALIZACJA BAZY SQLITE ---
DB_FILE = "lockpicks.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS lockpicks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nick TEXT,
                    rodzaj_zamka TEXT,
                    wynik TEXT,
                    czas REAL
                )''')
    conn.commit()
    conn.close()

# --- POBIERANIE LISTY I PLIKÓW Z FTP ---
def fetch_logs_from_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    print("[DEBUG] Połączono z FTP")

    try:
        files = ftp.nlst()
    except:
        files = []
        ftp.retrlines('LIST', files.append)
        files = [f.split()[-1] for f in files]

    log_files = [f for f in files if re.match(r"^gameplay_.*\.log$", f)]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików gameplay_*.log")
    entries = []

    for filename in log_files:
        lines = []
        ftp.retrlines(f"RETR {filename}", lines.append)
        for line in lines:
            match = re.search(r"Lockpicker: (.+?) próbował otworzyć zamek (.+?): (SUCCESS|FAIL) \(([\d.]+)s\)", line)
            if match:
                nick, lock_type, result, time_taken = match.groups()
                entries.append((nick, lock_type, result, float(time_taken)))
    ftp.quit()
    return entries

# --- ZAPIS DANYCH DO BAZY ---
def save_to_db(entries):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    new_count = 0
    for nick, lock_type, result, time_taken in entries:
        c.execute("SELECT COUNT(*) FROM lockpicks WHERE nick=? AND rodzaj_zamka=? AND wynik=? AND czas=?", 
                  (nick, lock_type, result, time_taken))
        if c.fetchone()[0] == 0:
            c.execute("INSERT INTO lockpicks (nick, rodzaj_zamka, wynik, czas) VALUES (?, ?, ?, ?)",
                      (nick, lock_type, result, time_taken))
            new_count += 1
    conn.commit()
    conn.close()
    print(f"[DEBUG] Dodano {new_count} nowych wpisów do bazy.")
    return new_count

# --- GENEROWANIE TABEL I WYSYŁANIE NA WEBHOOK ---
def generate_and_send_tables():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Admin table
    c.execute('''SELECT nick, rodzaj_zamka,
                        COUNT(*) as total,
                        SUM(CASE WHEN wynik='SUCCESS' THEN 1 ELSE 0 END) as success,
                        SUM(CASE WHEN wynik='FAIL' THEN 1 ELSE 0 END) as fail,
                        ROUND(100.0 * SUM(CASE WHEN wynik='SUCCESS' THEN 1 ELSE 0 END) / COUNT(*),1) as skutecznosc,
                        ROUND(AVG(czas),2) as avg_time
                 FROM lockpicks
                 GROUP BY nick, rodzaj_zamka
                 ORDER BY nick, rodzaj_zamka''')
    admin_rows = c.fetchall()
    admin_table = "**Admin**\nNick | Rodzaj zamka | Wszystkie próby | Udane | Nieudane | Skuteczność | Średni czas\n"
    for r in admin_rows:
        admin_table += f"{r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]}% | {r[6]}s\n"

    # Stats table
    stats_table = "**Statystyki**\nNick | Zamek | Skuteczność | Średni czas\n"
    for r in admin_rows:
        stats_table += f"{r[0]} | {r[1]} | {r[5]}% | {r[6]}s\n"

    # Podium table
    c.execute('''SELECT nick,
                        ROUND(100.0 * SUM(CASE WHEN wynik='SUCCESS' THEN 1 ELSE 0 END) / COUNT(*),1) as skutecznosc,
                        ROUND(AVG(czas),2) as avg_time
                 FROM lockpicks
                 GROUP BY nick
                 ORDER BY skutecznosc DESC, avg_time ASC''')
    podium_rows = c.fetchall()
    podium_table = "**Podium**\n🥇 | Nick | Skuteczność | Średni czas\n"
    medals = ['🥇','🥈','🥉']
    for i, r in enumerate(podium_rows):
        medal = medals[i] if i < 3 else ''
        podium_table += f"{medal} | {r[0]} | {r[1]}% | {r[2]}s\n"

    conn.close()

    payload = {"content": f"{admin_table}\n\n{stats_table}\n\n{podium_table}"}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Wysłano na webhook, status: {response.status_code}")

# --- GŁÓWNA PĘTLA ---
def main_loop():
    while True:
        print("[DEBUG] Rozpoczynam cykl pętli...")
        entries = fetch_logs_from_ftp()
        if entries:
            new_count = save_to_db(entries)
            if new_count > 0:
                generate_and_send_tables()
            else:
                print("[DEBUG] Brak nowych wpisów.")
        else:
            print("[DEBUG] Nie znaleziono żadnych wpisów.")
        time.sleep(60)

# --- FLASK KEEPALIVE ---
from flask import Flask
app = Flask(__name__)
@app.route('/')
def index():
    return "Lockpick Monitor is running."

# --- START ---
if __name__ == "__main__":
    init_db()
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
