import ftplib import os import re import psycopg2 import requests import threading import time import io import codecs from datetime import datetime from flask import Flask

=== KONFIGURACJE ===

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_HOST = "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech" DB_NAME = "neondb" DB_USER = "neondb_owner" DB_PASS = "npg_dRU1YCtxbh6v" DB_SSLMODE = "require" DB_PORT = 5432

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

=== FLASK ===

app = Flask(name)

@app.route("/") def index(): return "Alive"

=== BAZA DANYCH ===

def get_conn(): return psycopg2.connect( host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, sslmode=DB_SSLMODE, port=DB_PORT )

def init_db(): with get_conn() as conn: with conn.cursor() as cur: cur.execute(""" CREATE TABLE IF NOT EXISTS lockpicking ( nick TEXT, castle TEXT, result TEXT, time FLOAT, timestamp TIMESTAMP ) """) conn.commit() print("[INFO] Tabela lockpicking gotowa.")

=== FUNKCJE POMOCNICZE ===

def download_log_files(): print("[DEBUG] Łączenie z FTP...") ftp = ftplib.FTP() ftp.connect(FTP_HOST, FTP_PORT) ftp.login(FTP_USER, FTP_PASS) ftp.cwd(FTP_DIR) filenames = ftp.nlst("gameplay_*.log") print(f"[INFO] Znaleziono {len(filenames)} plików logów.") logs = [] for filename in filenames: r = io.BytesIO() ftp.retrbinary(f"RETR {filename}", r.write) content = codecs.decode(r.getvalue(), "utf-16le", errors="ignore") logs.append((filename, content)) print(f"[DEBUG] Pobrano: {filename}") ftp.quit() return logs

def parse_logs(logs): pattern = re.compile(r"(?P<timestamp>\d{4}.\d{2}.\d{2}-\d{2}\.\d{2}\.\d{2}).?Lockpicking: (?P<nick>.?) tried to break into (?P<castle>.*?) and (?P<result>succeeded|failed) in (?P<time>\d+(?:[.,]\d+)?)") entries = [] for filename, content in logs: for match in pattern.finditer(content): timestamp = datetime.strptime(match.group("timestamp"), "%Y.%m.%d-%H.%M.%S") nick = match.group("nick") castle = match.group("castle") result = match.group("result") time_val = match.group("time").replace(",", ".") try: time_float = float(time_val) except ValueError: print(f"[WARN] Nieprawidłowy czas: {time_val} w pliku {filename}") continue entries.append((nick, castle, result, time_float, timestamp)) print(f"[INFO] Przetworzono {len(entries)} wpisów z logów.") return entries

def save_to_db(entries): inserted = 0 try: with get_conn() as conn: with conn.cursor() as cur: for e in entries: cur.execute(""" SELECT 1 FROM lockpicking WHERE nick = %s AND castle = %s AND result = %s AND time = %s AND timestamp = %s """, e) if cur.fetchone() is None: cur.execute(""" INSERT INTO lockpicking (nick, castle, result, time, timestamp) VALUES (%s, %s, %s, %s, %s) """, e) inserted += 1 conn.commit() except Exception as ex: print(f"[ERROR] Błąd zapisu do bazy: {ex}") print(f"[INFO] Zapisano {inserted} nowych wpisów.") return inserted

def generate_report(): with get_conn() as conn: with conn.cursor() as cur: cur.execute(""" SELECT nick, castle, COUNT() as total, COUNT() FILTER (WHERE result = 'succeeded') as success, COUNT() FILTER (WHERE result = 'failed') as fail, ROUND(100.0 * COUNT() FILTER (WHERE result = 'succeeded') / NULLIF(COUNT(*), 0), 2) as effectiveness, ROUND(AVG(time), 2) as avg_time FROM lockpicking GROUP BY nick, castle ORDER BY effectiveness DESC """) rows = cur.fetchall() headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"] table = "| " + " | ".join(headers) + " |\n" table += "|" + "|".join([" :---: " for _ in headers]) + "|\n" for row in rows: table += "| " + " | ".join(str(col) for col in row) + " |\n" return table

def send_report(content): print("[DEBUG] Wysyłanie danych na webhook...") response = requests.post(WEBHOOK_URL, json={"content": f"\n{content}"}) if response.status_code == 204: print("[INFO] Raport wysłany.") else: print(f"[ERROR] Nie udało się wysłać raportu: {response.status_code}")

=== GŁÓWNA PĘTLA ===

def main_loop(): print("[DEBUG] Start main_loop") init_db() last_run = time.time() while True: logs = download_log_files() entries = parse_logs(logs) new_count = save_to_db(entries) if new_count > 0: report = generate_report() send_report(report) else: print("[INFO] Brak nowych wpisów.") time.sleep(60)

if name == "main": threading.Thread(target=main_loop).start() app.run(host='0.0.0.0', port=3000)

