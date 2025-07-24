# --- main.py ---

import os
import time
import threading
import sqlite3
from ftplib import FTP
import requests
from flask import Flask

# --- KONFIGURACJA FTP I WEBHOOK ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- INICJALIZACJA DB ---
DB_FILE = "logi.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS logi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nick TEXT,
            rodzaj_zamka TEXT,
            czas REAL,
            wynik TEXT,
            UNIQUE(nick, rodzaj_zamka, czas, wynik)
        )
    ''')
    conn.commit()
    conn.close()

def insert_entry(nick, rodzaj_zamka, czas, wynik):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO logi (nick, rodzaj_zamka, czas, wynik)
            VALUES (?, ?, ?, ?)
        ''', (nick, rodzaj_zamka, czas, wynik))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # wpis już istnieje
    conn.close()

def generate_tables_from_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # --- tabela Admin ---
    c.execute('''
        SELECT nick, rodzaj_zamka,
               COUNT(*) as wszystkie,
               SUM(CASE WHEN wynik="success" THEN 1 ELSE 0 END) as udane,
               SUM(CASE WHEN wynik="fail" THEN 1 ELSE 0 END) as nieudane,
               ROUND(SUM(CASE WHEN wynik="success" THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2) as skutecznosc,
               ROUND(AVG(czas),2) as sr_czas
        FROM logi
        GROUP BY nick, rodzaj_zamka
        ORDER BY nick, rodzaj_zamka
    ''')
    admin_table = "Admin:\nNick | Rodzaj zamka | Wszystkie | Udane | Nieudane | Skuteczność | Średni czas\n"
    for row in c.fetchall():
        admin_table += f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]}% | {row[6]}s\n"

    # --- tabela Statystyki ---
    stats_table = "Statystyki:\nNick | Zamek | Skuteczność | Średni czas\n"
    for row in c.execute('''
        SELECT nick, rodzaj_zamka,
               ROUND(SUM(CASE WHEN wynik="success" THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2) as skutecznosc,
               ROUND(AVG(czas),2) as sr_czas
        FROM logi
        GROUP BY nick, rodzaj_zamka
        ORDER BY nick, rodzaj_zamka
    '''):
        stats_table += f"{row[0]} | {row[1]} | {row[2]}% | {row[3]}s\n"

    # --- tabela Podium ---
    podium_table = "Podium:\n🏆 | Nick | Skuteczność | Średni czas\n"
    c.execute('''
        SELECT nick,
               ROUND(SUM(CASE WHEN wynik="success" THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2) as skutecznosc,
               ROUND(AVG(czas),2) as sr_czas
        FROM logi
        GROUP BY nick
        ORDER BY skutecznosc DESC
    ''')
    podium = c.fetchall()
    medals = ["🥇","🥈","🥉"]
    for i, row in enumerate(podium):
        medal = medals[i] if i < len(medals) else ""
        podium_table += f"{medal} | {row[0]} | {row[1]}% | {row[2]}s\n"

    conn.close()
    return admin_table, stats_table, podium_table

# --- FTP ---
def download_logs_from_ftp():
    print("[DEBUG] Łączenie z FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    files = ftp.nlst()
    log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    entries = []

    for filename in log_files:
        print(f"[INFO] Pobieram {filename}")
        lines = []
        ftp.retrlines(f"RETR {filename}", lines.append)
        for line in lines:
            parsed = parse_line(line)
            if parsed:
                entries.append(parsed)
    ftp.quit()
    return entries

# --- PARSER ---
def parse_line(line):
    # PRZYKŁADOWY FORMAT: "[nick] lockpick type=advanced time=3.5 result=success"
    if "lockpick" not in line:
        return None
    try:
        parts = line.split()
        nick = parts[0].strip("[]")
        rodzaj_zamka = parts[2].split("=")[1]
        czas = float(parts[3].split("=")[1])
        wynik = parts[4].split("=")[1]
        return nick, rodzaj_zamka, czas, wynik
    except Exception as e:
        print(f"[ERROR] Nie udało się sparsować linii: {line} ({e})")
        return None

# --- WEBHOOK ---
def send_webhook(admin_table, stats_table, podium_table):
    content = f"```\n{admin_table}\n\n{stats_table}\n\n{podium_table}\n```"
    resp = requests.post(WEBHOOK_URL, json={"content": content})
    print(f"[INFO] Webhook status: {resp.status_code}")

# --- GŁÓWNA PĘTLA ---
def main_loop():
    while True:
        entries = download_logs_from_ftp()
        for entry in entries:
            insert_entry(*entry)
        admin_table, stats_table, podium_table = generate_tables_from_db()
        send_webhook(admin_table, stats_table, podium_table)
        time.sleep(60)

# --- FLASK ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- START ---
if __name__ == "__main__":
    init_db()
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
