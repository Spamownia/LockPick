import re
import time
import threading
from ftplib import FTP
from flask import Flask
import requests

# === KONFIGURACJA ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "npg_dRU1YCtxbh6v"
LOG_PATH = "/SCUM/Saved/SaveFiles/Logs"
CHECK_INTERVAL = 15
WEBHOOK_URL = "https://discord.com/api/webhooks/1383407890663997450/hr2zvr2PjO20IDLIk5nZd8juZDxG9kYkOOZ0c2_sqzGtuXra8Dz-HbhtnhtF3Yb0Hsgi"

# === STAN ===
PROCESSED_FILES = set()

# === FLASK ===
app = Flask(__name__)
@app.route("/")
def index():
    return "LockpickingLogger is running."

def run_flask():
    app.run(host="0.0.0.0", port=8080)

# === FTP ===
def connect_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_PATH)
    return ftp

def list_log_files(ftp):
    return [f for f in ftp.nlst() if f.startswith("log") and f.endswith(".log")]

def read_log_file(ftp, filename):
    lines = []
    ftp.retrbinary(f"RETR {filename}", lambda data: lines.append(data))
    content = b"".join(lines).decode("windows-1250", errors="ignore")
    return content.splitlines()

# === PARSER + DISCORD ===
def parse_lockpicking_line(line):
    pattern = (
        r"\[LockpickingMinigame_C\] User: (?P<nick>.+?) \(\d+, (?P<steam_id>\d+)\)\. "
        r"Success: (?P<success>\w+)\. Elapsed time: (?P<elapsed>[\d.]+)\. "
        r"Failed attempts: (?P<fails>\d+)\. Target object: (?P<object>.+?)\(ID: (?P<id>\d+)\)\. "
        r"Lock type: (?P<lock_type>\w+)\. User owner: \d+\(\[(?P<owner_steam>\d+)\] (?P<owner_nick>.+?)\)\. "
        r"Location: X=(?P<x>[-\d.]+) Y=(?P<y>[-\d.]+) Z=(?P<z>[-\d.]+)"
    )
    match = re.search(pattern, line)
    if not match:
        print(f"⚠️ Nieparsowalna linia:\n{line}")
        return

    data = match.groupdict()
    print(f"🔓 {data['nick']} próbował lockpickingu ({data['success']})")

    embed = {
        "title": f"🔐 Lockpicking {'sukces' if data['success'] == 'true' else 'porażka'}",
        "color": 3066993 if data['success'] == 'true' else 15158332,
        "fields": [
            {"name": "Gracz", "value": f"{data['nick']} ({data['steam_id']})", "inline": True},
            {"name": "Czas", "value": f"{data['elapsed']}s", "inline": True},
            {"name": "Nieudane próby", "value": data['fails'], "inline": True},
            {"name": "Zamek", "value": data['lock_type'], "inline": True},
            {"name": "Obiekt", "value": data['object'], "inline": False},
            {"name": "Właściciel", "value": f"{data['owner_nick']} ({data['owner_steam']})", "inline": False},
            {"name": "Pozycja", "value": f"X={data['x']}, Y={data['y']}, Z={data['z']}", "inline": False}
        ],
        "footer": {"text": "SCUM Lockpicking Logger"},
    }

    try:
        requests.post(WEBHOOK_URL, json={"embeds": [embed]})
    except Exception as e:
        print(f"❌ Błąd wysyłania do Discorda: {e}")

# === PRZETWARZANIE ===
def process_lines(lines):
    for line in lines:
        if "[LogMinigame] [LockpickingMinigame_C]" in line:
            parse_lockpicking_line(line)

def scan_all_logs_at_startup():
    print("🔍 Skanowanie wszystkich logów przy starcie...")
    try:
        ftp = connect_ftp()
        log_files = list_log_files(ftp)
        for filename in log_files:
            content = read_log_file(ftp, filename)
            process_lines(content)
            PROCESSED_FILES.add(filename)
        ftp.quit()
    except Exception as e:
        print(f"❌ Błąd FTP (startup): {e}")

def monitor_ftp():
    print("🕵️‍♂️ Start monitorowania FTP...")
    while True:
        try:
            ftp = connect_ftp()
            current_files = list_log_files(ftp)
            new_files = [f for f in current_files if f not in PROCESSED_FILES]
            for filename in new_files:
                content = read_log_file(ftp, filename)
                process_lines(content)
                PROCESSED_FILES.add(filename)
            ftp.quit()
        except Exception as e:
            print(f"❌ Błąd monitorowania FTP: {e}")
        time.sleep(CHECK_INTERVAL)

# === START ===
if __name__ == "__main__":
    print("🚀 Start bota LockpickingLogger...")

    # Skan przy starcie
    scan_all_logs_at_startup()

    # Monitoring FTP
    monitor_thread = threading.Thread(target=monitor_ftp)
    monitor_thread.start()

    # Flask keep-alive
    run_flask()
