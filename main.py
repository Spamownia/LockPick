# --- AUTOMATYCZNA INSTALACJA (cicho) ---
import subprocess, sys

def silent_install(package):
    try:
        __import__(package)
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", package],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

for pkg in ["requests", "flask"]:
    silent_install(pkg)

# --- IMPORTY ---
import re
import csv
import statistics
import requests
from collections import defaultdict
from ftplib import FTP
from io import BytesIO
import os
import time
from flask import Flask

# --- FLASK PING ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive", 200

# --- FUNKCJA WYSYŁANIA NA DISCORD ---
def send_discord(content, webhook_url):
    requests.post(webhook_url, json={"content": content})

# --- KONFIGURACJA FTP ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

# --- WEBHOOKI ---
WEBHOOK_TABLE1 = WEBHOOK_TABLE2 = WEBHOOK_TABLE3 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- WZORZEC ---
pattern = re.compile(
    r"User: (?P<nick>[\w\d]+) \(\d+, [\d]+\)\. "
    r"Success: (?P<success>Yes|No)\. "
    r"Elapsed time: (?P<elapsed>[\d\.]+)\. "
    r"Failed attempts: (?P<failed_attempts>\d+)\. "
    r"Target object: [^\)]+\)\. "
    r"Lock type: (?P<lock_type>\w+)\."
)

lock_order = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4}
STATE_FILE = "last_state.txt"

# --- POBRANIE WSZYSTKICH LOGÓW PRZY PIERWSZYM URUCHOMIENIU ---
if not os.path.exists("logi.csv"):
    ftp = FTP()
    ftp.connect(FTP_IP, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)
    log_files = []
    ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
    log_files = [f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")]
    log_files.sort()

    data = defaultdict(lambda: {"all_attempts":0, "successful_attempts":0, "failed_attempts":0, "times":[]})

    for log_file in log_files:
        print(f"[INFO] Pierwsze pobieranie: {log_file}")
        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {log_file}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

        for match in pattern.finditer(log_text):
            nick = match.group("nick")
            lock_type = match.group("lock_type")
            success = match.group("success")
            elapsed = float(match.group("elapsed"))
            failed_attempts = int(match.group("failed_attempts"))

            key = (nick, lock_type)
            data[key]["all_attempts"] += 1
            if success == "Yes":
                data[key]["successful_attempts"] += 1
            else:
                data[key]["failed_attempts"] += 1
            data[key]["times"].append(elapsed)

    ftp.quit()

    # --- ZAPIS DO CSV (bez nagłówka na stałe) ---
    with open("logi.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        for (nick, lock_type), stats in data.items():
            all_attempts = stats["all_attempts"]
            successful_attempts = stats["successful_attempts"]
            failed_attempts = stats["failed_attempts"]
            avg_time = round(statistics.mean(stats["times"]),2)
            effectiveness = round(100 * successful_attempts / all_attempts,2)
            writer.writerow([nick, lock_type, all_attempts, successful_attempts, failed_attempts, effectiveness, avg_time])

# --- GŁÓWNA PĘTLA SPRAWDZANIA NOWYCH ZDARZEŃ ---
def main_loop():
    while True:
        ftp = FTP()
        ftp.connect(FTP_IP, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)
        log_files = []
        ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
        log_files = [f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")]
        log_files.sort()
        latest_log = log_files[-1]

        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {latest_log}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")
        ftp.quit()

        lines = log_text.splitlines()
        last_line = 0
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = f.read().strip().split("\n")
                if state[0] == latest_log:
                    last_line = int(state[1])

        new_lines = lines[last_line:]
        if not new_lines:
            print("[INFO] Brak nowych zdarzeń w logu.")
            time.sleep(60)
            continue

        # --- PRZETWARZANIE NOWYCH ZDARZEŃ ---
        with open("logi.csv", "a", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            new_events = 0
            for line in new_lines:
                match = pattern.search(line)
                if match:
                    nick = match.group("nick")
                    lock_type = match.group("lock_type")
                    success = match.group("success")
                    elapsed = float(match.group("elapsed"))
                    failed_attempts = int(match.group("failed_attempts"))
                    all_attempts = 1
                    successful_attempts = 1 if success == "Yes" else 0
                    failed = 0 if success == "Yes" else 1
                    effectiveness = 100 if success == "Yes" else 0
                    writer.writerow([nick, lock_type, all_attempts, successful_attempts, failed, effectiveness, elapsed])
                    new_events += 1

        with open(STATE_FILE, "w") as f:
            f.write(f"{latest_log}\n{len(lines)}")

        print(f"[INFO] Dodano {new_events} nowych zdarzeń. Generowanie tabel i wysyłka.")

        # --- GENEROWANIE I WYSYŁKA TABEL I PODIUM ---
        csv_rows = []
        with open("logi.csv", "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            csv_rows = list(reader)

        table_block = "```\n"
        table_block += f"{'Nick':<10} {'Zamek':<10} {'Wszystkie':<12} {'Udane':<6} {'Nieudane':<9} {'Skut.':<8} {'Śr. czas':<8}\n"
        table_block += "-" * 70 + "\n"
        for row in csv_rows:
            table_block += f"{row[0]:<10} {row[1]:<10} {row[2]:<12} {row[3]:<6} {row[4]:<9} {row[5]:<8} {row[6]:<8}\n"
        table_block += "```"
        send_discord(table_block, WEBHOOK_TABLE1)

        # --- PODIUM ---
        player_summary = defaultdict(lambda: {"all_attempts":0, "successful_attempts":0, "times":[]})
        for row in csv_rows:
            nick, lock_type, all_attempts, successful_attempts, failed_attempts, effectiveness, avg_time = row
            all_attempts = int(all_attempts)
            successful_attempts = int(successful_attempts)
            avg_time = float(avg_time)
            player_summary[nick]["all_attempts"] += all_attempts
            player_summary[nick]["successful_attempts"] += successful_attempts
            player_summary[nick]["times"].append(avg_time)

        podium = []
        for nick, stats in player_summary.items():
            all_attempts = stats["all_attempts"]
            successful_attempts = stats["successful_attempts"]
            eff = round(100 * successful_attempts / all_attempts, 2) if all_attempts else 0
            avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
            podium.append((nick, eff, avg))

        podium = sorted(podium, key=lambda x: (-x[1], x[2]))[:5]
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]

        podium_block = "```\n"
        podium_block += "           🏆 PODIUM           \n"
        podium_block += "--------------------------------\n"
        podium_block += f"{'Miejsce':<8} {'Nick':<10} {'Skuteczność':<12} {'Średni czas':<10}\n"
        for i, (nick, eff, avg) in enumerate(podium):
            podium_block += f"{medals[i]} {i+1:<6} {nick:<10} {eff:<12} {avg:<10}\n"
        podium_block += "```"
        send_discord(podium_block, WEBHOOK_TABLE3)

        time.sleep(60)

# --- START SERWERA I PĘTLI ---
if __name__ == "__main__":
    import threading
    threading.Thread(target=main_loop).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
