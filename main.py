import os
import re
import time
import ftplib
import psycopg2
import pandas as pd
from flask import Flask
from tabulate import tabulate
import requests
from io import BytesIO

# --- KONFIGURACJE ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOGS_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- APLIKACJA FLASK (ALIVE) ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- BAZA DANYCH ---
def init_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicking (
                    nick TEXT,
                    lock TEXT,
                    success BOOLEAN,
                    elapsed_time REAL
                )
            """)
            conn.commit()

def insert_data(entries):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for entry in entries:
                cur.execute("""
                    INSERT INTO lockpicking (nick, lock, success, elapsed_time)
                    VALUES (%s, %s, %s, %s)
                """, (entry['nick'], entry['lock'], entry['success'], entry['elapsed_time']))
            conn.commit()

def fetch_all_data():
    with psycopg2.connect(**DB_CONFIG) as conn:
        return pd.read_sql("SELECT * FROM lockpicking", conn)

# --- FTP ---
def list_log_files(ftp):
    files = []
    ftp.cwd(FTP_LOGS_PATH)
    ftp.retrlines("LIST", lambda line: files.append(line))
    log_files = [line.split()[-1] for line in files if line.split()[-1].startswith("gameplay_") and line.split()[-1].endswith(".log")]
    return log_files

def download_log_file(ftp, filename):
    bio = BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    content = bio.getvalue().decode("utf-16-le", errors="ignore")
    return content

# --- PARSER ---
def parse_log_content(content):
    lines = content.splitlines()
    print(f"[DEBUG] Liczba linii w logu: {len(lines)}")
    print(f"[DEBUG] Rozmiar zawartości po dekodowaniu: {len(content)} znaków")
    print("[DEBUG] Przykładowe linie z logu:")
    for i, line in enumerate(lines[:10]):
        print(f"  [{i+1}] {line}")

    pattern = re.compile(
        r'\[LogMinigame\] \[LockpickingMinigame_C\] User: (.+?) \(\.\.\.\) Lock: (.+?) Success: (Yes|No)\. Elapsed time: ([0-9.]+)'
    )
    results = []
    for line in lines:
        match = pattern.search(line)
        if match:
            nick = match.group(1).strip()
            lock = match.group(2).strip()
            success = match.group(3).strip() == "Yes"
            elapsed = float(match.group(4))
            results.append({
                "nick": nick,
                "lock": lock,
                "success": success,
                "elapsed_time": elapsed
            })
    print(f"[DEBUG] Rozpoznano wpisów: {len(results)}")
    return results

# --- WYNIKI ---
def create_dataframe(df):
    grouped = df.groupby(["nick", "lock"])
    results = []
    for (nick, lock), group in grouped:
        total = len(group)
        success = group["success"].sum()
        failed = total - success
        effectiveness = round(success / total * 100, 2)
        avg_time = round(group["elapsed_time"].mean(), 2)
        results.append([nick, lock, total, success, failed, f"{effectiveness}%", avg_time])
    return pd.DataFrame(results, columns=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"])

def send_to_discord(df):
    if df.empty:
        print("[INFO] Brak danych do wysłania.")
        return
    table = tabulate(df, headers='keys', tablefmt='grid', stralign='center', numalign='center')
    data = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=data)
    print(f"[INFO] Wysłano dane na Discord (status: {response.status_code})")

# --- GŁÓWNA PĘTLA ---
def main_loop():
    print("[DEBUG] Start main_loop")
    init_db()
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        log_files = list_log_files(ftp)
        print(f"[DEBUG] Znaleziono plików: {len(log_files)}")
        all_entries = []
        for file in log_files:
            print(f"[INFO] Przetwarzanie: {file}")
            content = download_log_file(ftp, file)
            entries = parse_log_content(content)
            if entries:
                insert_data(entries)
                all_entries.extend(entries)
        if all_entries:
            df = fetch_all_data()
            result_df = create_dataframe(df)
            send_to_discord(result_df)
        else:
            print("[INFO] Brak nowych danych.")

if __name__ == "__main__":
    main_loop()
    app.run(host='0.0.0.0', port=3000)
