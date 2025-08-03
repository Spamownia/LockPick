import os
import re
import time
import ftplib
import requests
from flask import Flask
from threading import Thread
from dotenv import load_dotenv

load_dotenv()

FTP_HOST = os.getenv("FTP_HOST")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_DIR = os.getenv("FTP_DIR", "/SCUM/Saved/SaveFiles/Logs")
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "15"))  # sekund

app = Flask(__name__)
last_seen_line = ""

LOG_LINE_REGEX = re.compile(
    r'(?P<timestamp>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): \[LogMinigame\] \[LockpickingMinigame_C\] '
    r'User: (?P<user>.+?) \(\d+, (?P<steamid>\d+)\)\. '
    r'Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d.]+)\. '
    r'Failed attempts: (?P<fails>\d+)\. Target object: (?P<object>.+?)\(ID: (?P<object_id>\d+)\)\. '
    r'Lock type: (?P<lock_type>\w+)\. User owner: \d+\(\[(?P<owner_steamid>\d+)\] (?P<owner>.+?)\)\. '
    r'Location: X=(?P<x>-?[\d.]+) Y=(?P<y>-?[\d.]+) Z=(?P<z>-?[\d.]+)'
)

def ftp_connect():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.encoding = "windows-1250"
    return ftp

def get_latest_log_file():
    ftp = ftp_connect()
    ftp.cwd(FTP_DIR)

    files = []
    ftp.retrlines("LIST", files.append)

    log_files = [f.split()[-1] for f in files if f.split()[-1].startswith("log_")]
    log_files.sort(reverse=True)

    if not log_files:
        ftp.quit()
        return None, []

    latest = log_files[0]
    lines = []
    ftp.retrlines(f"RETR {latest}", lines.append)
    ftp.quit()
    return latest, lines

def send_to_discord(data):
    embed = {
        "title": f"🔐 Lockpicking Attempt - {data['user']}",
        "color": 0x3498db if data['success'] == "Yes" else 0xe74c3c,
        "fields": [
            {"name": "👤 Użytkownik", "value": data['user'], "inline": True},
            {"name": "✅ Sukces", "value": data['success'], "inline": True},
            {"name": "⏱️ Czas", "value": f"{data['time']}s", "inline": True},
            {"name": "🔒 Próby", "value": data['fails'], "inline": True},
            {"name": "🎯 Obiekt", "value": data['object'], "inline": True},
            {"name": "🔧 Typ zamka", "value": data['lock_type'], "inline": True},
            {"name": "📦 Właściciel", "value": data['owner'], "inline": True},
            {"name": "📍 Lokalizacja", "value": f"X={data['x']} Y={data['y']} Z={data['z']}", "inline": False},
        ],
        "timestamp": f"{data['timestamp'].replace('.', '-', 2).replace('.', ':', 2)}"
    }

    payload = {
        "username": "LockpickingLogger",
        "embeds": [embed]
    }

    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code != 204:
            print(f"❌ Błąd wysyłania webhooka: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Wyjątek podczas wysyłania webhooka: {e}")

def poll_logs():
    global last_seen_line

    print("🕵️‍♂️ Start monitorowania FTP...")

    while True:
        try:
            filename, log_lines = get_latest_log_file()
            if not filename or not log_lines:
                print("📄 Brak logów.")
                time.sleep(SCAN_INTERVAL)
                continue

            new_lines = []
            if last_seen_line in log_lines:
                index = log_lines.index(last_seen_line) + 1
                new_lines = log_lines[index:]
            else:
                new_lines = log_lines[-50:]  # startowo tylko ostatnie 50 linii

            for line in new_lines:
                if "[LockpickingMinigame_C]" in line:
                    match = LOG_LINE_REGEX.match(line)
                    if match:
                        data = match.groupdict()
                        print(f"🔐 Log: {data}")
                        send_to_discord(data)
                    else:
                        print(f"⚠️ Niedopasowana linia: {line}")

            if log_lines:
                last_seen_line = log_lines[-1]

        except Exception as e:
            print(f"❌ Błąd: {e}")

        time.sleep(SCAN_INTERVAL)

@app.route("/")
def home():
    return "LockpickingLogger działa! 🔐"

if __name__ == "__main__":
    print("🚀 Start bota LockpickingLogger...")
    Thread(target=poll_logs, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
