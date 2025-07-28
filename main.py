import os
import re
import time
import pandas as pd
import psycopg2
import requests
from ftplib import FTP
from tabulate import tabulate
from flask import Flask

# Konfiguracja
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

def init_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpick_logs (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    lock_type TEXT,
                    success BOOLEAN,
                    elapsed_time FLOAT
                );
            """)
            conn.commit()

def parse_log_content(content):
    pattern = re.compile(
        r'\[LogMinigame\].*?User:\s*(?P<nick>\w+).*?Lock: (?P<lock>.*?)\..*?Success: (?P<success>\w+).*?Elapsed time: (?P<time>[\d.]+)',
        re.DOTALL
    )
    entries = []
    for match in pattern.finditer(content):
        entries.append({
            "nick": match.group("nick"),
            "lock_type": match.group("lock"),
            "success": match.group("success") == "Yes",
            "elapsed_time": float(match.group("time"))
        })
    return entries

def fetch_log_files():
    print("[DEBUG] Nawiązywanie połączenia FTP...", flush=True)
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    files = []
    ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
    log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

    print(f"[DEBUG] Znaleziono plików logów: {len(log_files)}", flush=True)

    all_entries = []
    for filename in log_files:
        print(f"[DEBUG] Przetwarzanie pliku: {filename}", flush=True)
        try:
            with open(f"/tmp/{filename}", "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
            with open(f"/tmp/{filename}", "r", encoding="utf-16-le") as f:
                content = f.read()
            entries = parse_log_content(content)
            print(f"[DEBUG] Rozpoznano wpisów: {len(entries)}", flush=True)
            all_entries.extend(entries)
        except Exception as e:
            print(f"[ERROR] Błąd przy przetwarzaniu {filename}: {e}", flush=True)

    ftp.quit()
    return all_entries

def save_to_db(entries):
    if not entries:
        print("[DEBUG] Brak nowych wpisów do zapisania.", flush=True)
        return

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for entry in entries:
                cur.execute("""
                    INSERT INTO lockpick_logs (nick, lock_type, success, elapsed_time)
                    VALUES (%s, %s, %s, %s)
                """, (entry["nick"], entry["lock_type"], entry["success"], entry["elapsed_time"]))
        conn.commit()
        print(f"[DEBUG] Zapisano wpisów do bazy: {len(entries)}", flush=True)

def create_dataframe():
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql("SELECT * FROM lockpick_logs", conn)

    if df.empty:
        return None

    grouped = df.groupby(["nick", "lock_type"])
    summary = grouped.agg(
        total=("success", "count"),
        successes=("success", "sum"),
        failures=("success", lambda x: (~x).sum()),
        accuracy=("success", "mean"),
        avg_time=("elapsed_time", "mean")
    ).reset_index()

    summary["accuracy"] = (summary["accuracy"] * 100).round(1).astype(str) + "%"
    summary["avg_time"] = summary["avg_time"].round(2).astype(str) + "s"

    return summary

def format_table(df):
    if df is None or df.empty:
        return "Brak danych do wyświetlenia."
    df.columns = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    return tabulate(df, headers="keys", tablefmt="grid", stralign="center", numalign="center")

def send_to_discord(table):
    print("\n[TABELA DO WYSYŁKI]\n", table, flush=True)
    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[DEBUG] Tabela wysłana poprawnie na Discord.", flush=True)
    else:
        print(f"[ERROR] Błąd przy wysyłaniu na Discord: {response.status_code}", flush=True)

# Flask do pingowania
app = Flask(__name__)
@app.route("/")
def index():
    return "Alive"

def main_loop():
    print("[DEBUG] Start programu", flush=True)
    init_db()
    entries = fetch_log_files()
    save_to_db(entries)
    df = create_dataframe()
    table = format_table(df)
    send_to_discord(table)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
