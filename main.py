# main.py

import ftplib
import os
import time
import io
import pandas as pd
import threading
from flask import Flask

# --- Konfiguracja FTP ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"

# --- Webhook Discord ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Flask app ---
app = Flask(__name__)

@app.route("/")
def index():
    return "Alive", 200

# --- Pobieranie logów FTP ---
def fetch_logs():
    logs = []
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd("/SCUM/Saved/SaveFiles/Logs")
            filenames = ftp.nlst()
            for filename in filenames:
                if filename.startswith("gameplay_") and filename.endswith(".log"):
                    print(f"[INFO] Downloading: {filename}")
                    with io.BytesIO() as bio:
                        ftp.retrbinary(f"RETR {filename}", bio.write)
                        content = bio.getvalue().decode("utf-8")
                        logs.append(content)
    except Exception as e:
        print(f"[ERROR] FTP: {e}")
    return logs

# --- Parsowanie lockpicków ---
def parse_lockpicks(logs):
    import re
    stats = {}
    pattern = re.compile(r"LOCKPICK: (\S+) - (\S+) - (\S+) - (\S+)s")
    for log in logs:
        for line in log.splitlines():
            m = pattern.search(line)
            if m:
                player, target, item, elapsed = m.groups()
                key = (player, target, item)
                if key not in stats:
                    stats[key] = {"count": 0, "times": []}
                stats[key]["count"] += 1
                try:
                    elapsed_clean = elapsed.replace(".", "", elapsed.count(".") - 1)  # usuwa nadmiar kropki
                    stats[key]["times"].append(float(elapsed_clean))
                except ValueError:
                    print(f"[WARN] Cannot convert elapsed: {elapsed}")
    return stats

# --- Budowanie tabeli ---
def build_table(stats):
    data = []
    for (player, target, item), v in stats.items():
        avg_time = round(sum(v["times"]) / len(v["times"]), 2) if v["times"] else 0
        data.append({
            "Player": player,
            "Target": target,
            "Item": item,
            "Count": v["count"],
            "AvgTime": avg_time
        })
    df = pd.DataFrame(data)
    file_name = "lockpicks.xlsx"
    df.to_excel(file_name, index=False)
    return file_name

# --- Wysyłka na Discord ---
def send_to_discord(file_path):
    import requests
    with open(file_path, "rb") as f:
        files = {"file": (file_path, f)}
        response = requests.post(WEBHOOK_URL, files=files)
        if response.status_code == 204:
            print("[INFO] Sent to Discord successfully.")
        else:
            print(f"[ERROR] Discord: {response.status_code} {response.text}")

# --- Pętla główna ---
def main_loop():
    while True:
        print("[INFO] Sprawdzanie logów FTP...")
        logs = fetch_logs()
        if logs:
            stats = parse_lockpicks(logs)
            excel_file = build_table(stats)
            send_to_discord(excel_file)
        else:
            print("[INFO] Brak nowych logów.")
        time.sleep(60)

# --- Uruchomienie ---
if __name__ == "__main__":
    # Start pętli głównej w wątku
    thread = threading.Thread(target=main_loop, daemon=True)
    thread.start()

    # Start Flask
    app.run(host="0.0.0.0", port=3000)
