import os
import re
import time
import ftplib
import threading
from flask import Flask
import requests

# === KONFIGURACJA ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "TwojeHasloTutaj"  # <- Uzupełnij danymi
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs"

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1383407890663997450/hr2zvr2PjO20IDLIk5nZd8juZDxG9kYkOOZ0c2_sqzGtuXra8Dz-HbhtnhtF3Yb0Hsgi"

CHECK_INTERVAL = 15
ENCODING = "windows-1250"

# =====================

app = Flask(__name__)
processed_lines = set()

def send_to_discord(message):
    payload = {"content": message}
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Błąd wysyłania do Discorda: {e}")

def extract_lockpicking_data(line):
    if "[LogMinigame] [LockpickingMinigame_C]" not in line:
        return None
    return line.strip()

def parse_log_file(ftp, filename):
    lines_to_process = []
    try:
        ftp.cwd(FTP_LOG_DIR)
        with open("temp.log", "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)

        with open("temp.log", "r", encoding=ENCODING, errors="ignore") as f:
            for line in f:
                if "[LogMinigame] [LockpickingMinigame_C]" in line:
                    clean = extract_lockpicking_data(line)
                    if clean and clean not in processed_lines:
                        processed_lines.add(clean)
                        lines_to_process.append(clean)
    except Exception as e:
        print(f"⚠️ Błąd odczytu {filename}: {e}")
    return lines_to_process

def scan_all_logs_on_startup():
    print("🔍 Skanowanie wszystkich logów przy starcie...")
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_LOG_DIR)
            files = ftp.nlst()
            log_files = [f for f in files if f.endswith(".log")]
            print(f"📄 Znalezione pliki: {log_files}")
            for log_file in log_files:
                entries = parse_log_file(ftp, log_file)
                for entry in entries:
                    send_to_discord(f"🧷 Lockpicking (archiwum): {entry}")
    except Exception as e:
        print(f"❌ Błąd FTP (startup): {e}")

def monitor_logs():
    while True:
        try:
            with ftplib.FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
                ftp.login(FTP_USER, FTP_PASS)
                ftp.cwd(FTP_LOG_DIR)
                files = ftp.nlst()
                log_files = [f for f in files if f.endswith(".log")]
                for log_file in log_files:
                    entries = parse_log_file(ftp, log_file)
                    for entry in entries:
                        send_to_discord(f"🧷 Lockpicking: {entry}")
        except Exception as e:
            print(f"❌ Błąd monitorowania FTP: {e}")
        time.sleep(CHECK_INTERVAL)

@app.route("/")
def index():
    return "LockpickingLogger is running."

if __name__ == "__main__":
    print("🚀 Start bota LockpickingLogger...")
    scan_all_logs_on_startup()  # uruchamiamy jednorazowe skanowanie
    print("🕵️‍♂️ Start monitorowania FTP...")
    threading.Thread(target=monitor_logs, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
