# --- AUTOMATYCZNA INSTALACJA (cicho) ---
import subprocess
import sys

def silent_install(package):
    try:
        __import__(package)
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", package],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

silent_install("requests")
silent_install("flask")

# --- IMPORTY ---
import re
import csv
import statistics
import requests
from collections import defaultdict
from ftplib import FTP
from io import BytesIO
import os
import threading
import time
from flask import Flask

# --- KONFIGURACJA FLASK ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- FUNKCJA WYSYŁANIA NA DISCORD ---
def send_discord(content, webhook_url):
    print("[DEBUG] Wysyłanie na webhook...")
    requests.post(webhook_url, json={"content": content})

# --- KONFIGURACJA FTP ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

# --- WEBHOOKI ---
WEBHOOK_TABLE1 = "https://discord.com/api/webhooks/xxx"
WEBHOOK_TABLE2 = WEBHOOK_TABLE1
WEBHOOK_TABLE3 = WEBHOOK_TABLE1

# --- WZORZEC ---
pattern = re.compile(
    r"User: (?P<nick>[\w\d]+) \(\d+, [\d]+\)\. "
    r"Success: (?P<success>Yes|No)\. "
    r"Elapsed time: (?P<elapsed>[\d\.]+)\. "
    r"Failed attempts: (?P<failed_attempts>\d+)\. "
    r"Target object: [^\)]+\)\. "
    r"Lock type: (?P<lock_type>\w+)\."
)

# --- KOLEJNOŚĆ ZAMKÓW ---
lock_order = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4}

# --- FUNKCJA GŁÓWNA ---
def process_logs():
    print("[DEBUG] Rozpoczynam przetwarzanie logów...")

    ftp = FTP()
    ftp.connect(FTP_IP, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)

    log_files = []
    ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
    log_files = [f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")]
    if not log_files:
        print("[ERROR] Brak plików gameplay_*.log na FTP.")
        ftp.quit()
        return

    latest_log = sorted(log_files)[-1]
    print(f"[INFO] Przetwarzanie najnowszego logu: {latest_log}")

    with BytesIO() as bio:
        ftp.retrbinary(f"RETR {latest_log}", bio.write)
        log_text = bio.getvalue().decode("utf-16-le", errors="ignore")
    ftp.quit()

    # --- Wczytywanie dotychczasowych danych ---
    history_data = {}
    if os.path.isfile("logi.csv"):
        with open("logi.csv", newline='', encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) < 7 or not row[0]: continue
                nick, lock_type = row[0], row[1]
                all_attempts = int(row[2])
                successful_attempts = int(row[3])
                failed_attempts = int(row[4])
                avg_time = float(row[6].strip('s'))
                history_data[(nick, lock_type)] = {
                    "all_attempts": all_attempts,
                    "successful_attempts": successful_attempts,
                    "failed_attempts": failed_attempts,
                    "times": [avg_time]*all_attempts
                }
    print(f"[DEBUG] Wczytano {len(history_data)} rekordów z historii.")

    # --- Parsowanie najnowszego logu ---
    current_data = {}
    for match in pattern.finditer(log_text):
        nick = match.group("nick")
        lock_type = match.group("lock_type")
        success = match.group("success")
        elapsed = float(match.group("elapsed"))

        key = (nick, lock_type)
        if key not in current_data:
            current_data[key] = {
                "all_attempts": 0,
                "successful_attempts": 0,
                "failed_attempts": 0,
                "times": [],
            }

        current_data[key]["all_attempts"] += 1
        if success == "Yes":
            current_data[key]["successful_attempts"] += 1
        else:
            current_data[key]["failed_attempts"] += 1

        current_data[key]["times"].append(elapsed)

    print(f"[DEBUG] Znaleziono {len(current_data)} rekordów w aktualnym logu.")

    if not current_data:
        print("[INFO] Brak nowych zdarzeń. Pomijam wysyłkę.")
        return

    # --- Sumowanie dotychczasowych i aktualnych danych ---
    combined_data = {}
    for key in set(history_data.keys()).union(current_data.keys()):
        hist = history_data.get(key, {"all_attempts":0,"successful_attempts":0,"failed_attempts":0,"times":[]})
        curr = current_data.get(key, {"all_attempts":0,"successful_attempts":0,"failed_attempts":0,"times":[]})

        all_attempts = hist["all_attempts"] + curr["all_attempts"]
        successful_attempts = hist["successful_attempts"] + curr["successful_attempts"]
        failed_attempts = hist["failed_attempts"] + curr["failed_attempts"]
        times = hist["times"] + curr["times"]

        combined_data[key] = {
            "all_attempts": all_attempts,
            "successful_attempts": successful_attempts,
            "failed_attempts": failed_attempts,
            "times": times
        }

    # --- Generowanie rankingów per gracz (sumarycznie) ---
    player_summary = {}
    for (nick, lock_type), stats in combined_data.items():
        if nick not in player_summary:
            player_summary[nick] = {"all":0, "succ":0, "times":[]}
        player_summary[nick]["all"] += stats["all_attempts"]
        player_summary[nick]["succ"] += stats["successful_attempts"]
        player_summary[nick]["times"].extend(stats["times"])

    # --- Sortowanie do tabel ---
    sorted_data = sorted(combined_data.items(), key=lambda x: (x[0][0], lock_order.get(x[0][1], 99)))

    # --- TABELA PODIUM ---
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    podium = []
    for nick, sums in player_summary.items():
        eff = round(100 * sums["succ"] / sums["all"], 2) if sums["all"] else 0
        avg_time = round(statistics.mean(sums["times"]), 2) if sums["times"] else 0
        podium.append((nick, eff, avg_time))

    podium_sorted = sorted(podium, key=lambda x: (-x[1], x[2]))[:5]

    # --- Wysyłka tabel na Discord ---
    # Podium
    col_widths = [2, 8, 12, 12]
    podium_block = "```\n"
    podium_block += f"{'':<{col_widths[0]}}{'Miejsce':^{col_widths[1]}}{'Skut.':^{col_widths[2]}}{'Śr. czas':^{col_widths[3]}}\n"
    podium_block += "-" * sum(col_widths) + "\n"
    for i, (nick, eff, avg) in enumerate(podium_sorted):
        medal = medals[i]
        podium_block += f"{medal:<{col_widths[0]}}{nick:^{col_widths[1]}}{(str(eff)+'%'):^{col_widths[2]}}{(str(avg)+'s'):^{col_widths[3]}}\n"
    podium_block += "```"
    send_discord(podium_block, WEBHOOK_TABLE3)

    print("[INFO] Wysłano tabelę podium na Discord.")

# --- FUNKCJA GŁÓWNEJ PĘTLI ---
def main_loop():
    while True:
        process_logs()
        time.sleep(60)

# --- START SERWERA I PĘTLI ---
if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
