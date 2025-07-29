import os
import time
import re
import ftplib
import io
import pandas as pd
import psycopg2
from tabulate import tabulate
import requests
from flask import Flask

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)
last_known_line = ""


@app.route('/')
def index():
    return "Alive"


def parse_log_content(content):
    pattern = re.compile(
        r"User: (?P<nick>\w+).*?Lock: (?P<lock>[A-Za-z_]+).*?Success: (?P<success>Yes|No).*?Elapsed time: (?P<time>\d+\.\d+)",
        re.DOTALL,
    )
    entries = []
    for match in pattern.finditer(content):
        entries.append(match.groupdict())
    return entries


def connect_to_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    return ftp


def get_latest_log_file(ftp):
    files = []
    ftp.retrlines("LIST", lambda line: files.append(line))
    log_files = [line.split()[-1] for line in files if line.split()[-1].startswith("gameplay_") and line.endswith(".log")]
    if not log_files:
        return None
    latest = sorted(log_files)[-1]
    print(f"[DEBUG] Najnowszy log: {latest}")
    return latest


def download_log_file(ftp, filename):
    with io.BytesIO() as bio:
        ftp.retrbinary(f"RETR {filename}", bio.write)
        return bio.getvalue().decode("utf-16le")


def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            nick TEXT,
            lock TEXT,
            success BOOLEAN,
            time FLOAT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def insert_new_entries(entries):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    new_count = 0
    for entry in entries:
        cur.execute("""
            SELECT 1 FROM lockpick_logs
            WHERE nick=%s AND lock=%s AND success=%s AND time=%s
            """, (entry["nick"], entry["lock"], entry["success"] == "Yes", float(entry["time"])))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO lockpick_logs (nick, lock, success, time)
                VALUES (%s, %s, %s, %s)
            """, (entry["nick"], entry["lock"], entry["success"] == "Yes", float(entry["time"])))
            new_count += 1
    conn.commit()
    cur.close()
    conn.close()
    return new_count


def create_dataframe():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM lockpick_logs", conn)
    conn.close()
    if df.empty:
        return None

    df["success"] = df["success"].astype(bool)
    grouped = df.groupby(["nick", "lock"])
    result = grouped.agg(
        total_tries=pd.NamedAgg(column="success", aggfunc="count"),
        successes=pd.NamedAgg(column="success", aggfunc="sum"),
        fails=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
        efficiency=pd.NamedAgg(column="success", aggfunc=lambda x: round(100 * x.sum() / x.count(), 2)),
        avg_time=pd.NamedAgg(column="time", aggfunc="mean")
    ).reset_index()

    result["avg_time"] = result["avg_time"].round(2)
    return result


def send_to_discord(df):
    if df is None or df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return
    table = tabulate(df, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"],
                     tablefmt="grid", stralign="center", numalign="center")
    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table}\n```"})
    if response.status_code == 204:
        print("[DEBUG] Wysłano dane na Discord webhook.")
    else:
        print(f"[ERROR] Błąd wysyłania na Discord: {response.status_code} - {response.text}")


def main_loop():
    global last_known_line
    print("[DEBUG] Start programu")
    init_db()

    while True:
        try:
            print("[DEBUG] Iteracja pętli sprawdzania logów FTP...")
            ftp = connect_to_ftp()
            latest_file = get_latest_log_file(ftp)
            if latest_file:
                content = download_log_file(ftp, latest_file)
                new_content = content if last_known_line not in content else content.split(last_known_line, 1)[-1]
                new_entries = parse_log_content(new_content)

                if new_entries:
                    print(f"[DEBUG] Znaleziono {len(new_entries)} nowych wpisów w {latest_file}")
                    last_known_line = content.strip().splitlines()[-1]  # aktualizuj ostatni wiersz
                    new_count = insert_new_entries(new_entries)
                    print(f"[DEBUG] Dodano {new_count} unikalnych wpisów do bazy.")
                    df = create_dataframe()
                    send_to_discord(df)
                else:
                    print("[DEBUG] Brak nowych wpisów lockpicking.")
            else:
                print("[DEBUG] Nie znaleziono logów gameplay_*")
            ftp.quit()
        except Exception as e:
            print(f"[ERROR] Wystąpił błąd: {e}")
        time.sleep(60)


if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
