import os
import re
import time
import pandas as pd
import psycopg2
import requests
from tabulate import tabulate
from ftplib import FTP_TLS
from io import BytesIO, StringIO
from flask import Flask

app = Flask(__name__)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASSWORD = os.getenv("FTP_PASSWORD")

def extract_log_entries(log_content):
    pattern = r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}: \[LogMinigame] \[LockpickingMinigame_C] User: .*?Location: X=.*"
    matches = re.findall(pattern, log_content, re.DOTALL)
    print(f"[DEBUG] Found {len(matches)} matching log entries")
    return matches

def parse_log_entry(entry):
    pattern = (
        r"(?P<timestamp>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): .*?User: (?P<user>.*?) \("
        r"(?P<user_id>\d+), (?P<steam_id>\d+)\)\. Success: (?P<success>\w+)\. Elapsed time: (?P<elapsed_time>[\d.]+)"
        r"\. Failed attempts: (?P<failed_attempts>\d+)\. Target object: (?P<target>.*?)\(ID: (?P<object_id>\d+)\)\."
        r" Lock type: (?P<lock_type>.*?)\. User owner: (?P<owner_id>\d+)\(\[(?P<owner_steam_id>\d+)] (?P<owner_name>.*?)\)\."
        r" Location: X=(?P<x>[-\d.]+) Y=(?P<y>[-\d.]+) Z=(?P<z>[-\d.]+)"
    )
    match = re.match(pattern, entry)
    if match:
        print(f"[DEBUG] Parsed log entry for user: {match.group('user')}")
        return match.groupdict()
    else:
        print("[DEBUG] Failed to parse log entry")
        return None

def fetch_log_from_ftp():
    print("[DEBUG] Connecting to FTP...")
    ftps = FTP_TLS(FTP_HOST)
    ftps.login(FTP_USER, FTP_PASSWORD)
    ftps.prot_p()

    files = ftps.nlst()
    log_files = [f for f in files if f.endswith(".log")]
    if not log_files:
        print("[DEBUG] No log files found")
        return None

    latest_file = sorted(log_files)[-1]
    print(f"[DEBUG] Fetching file: {latest_file}")
    bio = BytesIO()
    ftps.retrbinary(f"RETR {latest_file}", bio.write)
    bio.seek(0)
    content = bio.read().decode("utf-8", errors="ignore")
    ftps.quit()
    return content

def insert_into_db(df):
    print("[DEBUG] Inserting data into database...")
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_logs (
            timestamp TEXT, user TEXT, user_id INT, steam_id BIGINT,
            success TEXT, elapsed_time FLOAT, failed_attempts INT,
            target TEXT, object_id BIGINT, lock_type TEXT,
            owner_id INT, owner_steam_id BIGINT, owner_name TEXT,
            x FLOAT, y FLOAT, z FLOAT
        )
    """)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO lockpick_logs VALUES (
                %(timestamp)s, %(user)s, %(user_id)s, %(steam_id)s,
                %(success)s, %(elapsed_time)s, %(failed_attempts)s,
                %(target)s, %(object_id)s, %(lock_type)s,
                %(owner_id)s, %(owner_steam_id)s, %(owner_name)s,
                %(x)s, %(y)s, %(z)s
            )
        """, row)
    conn.commit()
    cur.close()
    conn.close()
    print("[DEBUG] Data inserted successfully")

def main_loop():
    print("[DEBUG] Start main_loop")
    while True:
        try:
            log_content = fetch_log_from_ftp()
            if log_content:
                entries = extract_log_entries(log_content)
                data = []
                for entry in entries:
                    parsed = parse_log_entry(entry)
                    if parsed:
                        try:
                            parsed["user_id"] = int(parsed["user_id"])
                            parsed["steam_id"] = int(parsed["steam_id"])
                            parsed["elapsed_time"] = float(parsed["elapsed_time"])
                            parsed["failed_attempts"] = int(parsed["failed_attempts"])
                            parsed["object_id"] = int(parsed["object_id"])
                            parsed["owner_id"] = int(parsed["owner_id"])
                            parsed["owner_steam_id"] = int(parsed["owner_steam_id"])
                            parsed["x"] = float(parsed["x"])
                            parsed["y"] = float(parsed["y"])
                            parsed["z"] = float(parsed["z"])
                            data.append(parsed)
                        except ValueError as e:
                            print(f"[ERROR] Type conversion error: {e}")
                if data:
                    df = pd.DataFrame(data)
                    print("[DEBUG] DataFrame created:")
                    print(tabulate(df, headers="keys", tablefmt="grid"))
                    insert_into_db(df)
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
        time.sleep(300)  # 5 minut

@app.route("/")
def index():
    return "Lockpick log parser is running."

if __name__ == "__main__":
    import threading
    threading.Thread(target=main_loop).start()
    app.run(host="0.0.0.0", port=3000)
