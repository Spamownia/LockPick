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
import json
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
WEBHOOK_TABLE1 = "https://discord.com/api/webhooks/..."
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

# --- ŁADOWANIE PRZETWORZONYCH LINII Z PLIKU ---
processed_lines_file = "processed_lines.json"
if os.path.isfile(processed_lines_file):
    with open(processed_lines_file, "r", encoding="utf-8") as f:
        processed_lines = set(json.load(f))
else:
    processed_lines = set()

# --- BLOKADA DLA JEDNOCZESNEGO PRZETWARZANIA ---
process_lock = threading.Lock()

# --- FUNKCJA GŁÓWNA ---
def process_logs():
    with process_lock:
        global processed_lines

        print("[DEBUG] Rozpoczynam przetwarzanie logów...")

        ftp = FTP()
        ftp.connect(FTP_IP, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)

        log_files = []
        ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
        log_files = sorted([f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")])

        if not log_files:
            print("[ERROR] Brak plików gameplay_*.log na FTP.")
            ftp.quit()
            return

        latest_log = log_files[-1]
        print(f"[INFO] Przetwarzanie logu: {latest_log}")

        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {latest_log}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")
        ftp.quit()

        new_events = []
        for line in log_text.splitlines():
            if line not in processed_lines:
                processed_lines.add(line)
                new_events.append(line)

        if not new_events:
            print("[INFO] Brak nowych zdarzeń w logu.")
            return

        data = {}
        user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})

        # --- Parsowanie nowych zdarzeń ---
        for entry in new_events:
            match = pattern.search(entry)
            if match:
                nick = match.group("nick")
                lock_type = match.group("lock_type")
                success = match.group("success")
                elapsed = float(match.group("elapsed"))

                # Sumowanie dla podium
                user_summary[nick]["total"] += 1
                user_summary[nick]["times"].append(elapsed)
                if success == "Yes":
                    user_summary[nick]["success"] += 1

                key = (nick, lock_type)
                if key not in data:
                    data[key] = {
                        "all_attempts": 0,
                        "successful_attempts": 0,
                        "failed_attempts": 0,
                        "times": [],
                    }

                data[key]["all_attempts"] += 1
                if success == "Yes":
                    data[key]["successful_attempts"] += 1
                else:
                    data[key]["failed_attempts"] += 1

                data[key]["times"].append(elapsed)

        print(f"[DEBUG] Przetworzono {len(new_events)} nowych wpisów.")

        # --- ZAPIS DOPISUJĄCY DO CSV ---
        file_exists = os.path.isfile("logi.csv")
        with open("logi.csv", "a", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "Nick", "Rodzaj zamka", "Ilość wszystkich prób", "Ilość udanych prób",
                    "Ilość nieudanych prób", "Skuteczność", "Śr. czas"
                ])
            for (nick, lock_type), stats in data.items():
                all_attempts = stats["all_attempts"]
                succ = stats["successful_attempts"]
                fail = stats["failed_attempts"]
                avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
                eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
                writer.writerow([nick, lock_type, all_attempts, succ, fail, f"{eff}%", f"{avg}s"])

        # --- TABELA PODIUM (sumy per gracz) ---
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        ranking = []
        for nick, summary in user_summary.items():
            total_attempts = summary["total"]
            total_success = summary["success"]
            times_all = summary["times"]

            eff = round(100 * total_success / total_attempts, 2) if total_attempts else 0
            avg = round(statistics.mean(times_all), 2) if times_all else 0

            ranking.append((nick, eff, avg))

        ranking = sorted(ranking, key=lambda x: (-x[1], x[2]))[:5]

        col_widths = [2, 10, 14, 14]
        podium_block = "```\n"
        podium_block += f"{'':<{col_widths[0]}}{'Nick':^{col_widths[1]}}{'Skuteczność':^{col_widths[2]}}{'Śr. czas':^{col_widths[3]}}\n"
        podium_block += "-" * sum(col_widths) + "\n"

        for i, (nick, eff, avg) in enumerate(ranking):
            medal = medals[i]
            podium_block += f"{medal:<{col_widths[0]}}{nick:^{col_widths[1]}}{(str(eff)+'%'):^{col_widths[2]}}{(str(avg)+'s'):^{col_widths[3]}}\n"
        podium_block += "```"
        send_discord(podium_block, WEBHOOK_TABLE3)
        print("[INFO] Wysłano tabelę podium.")

        # --- ZAPIS PRZETWORZONYCH LINII DO PLIKU ---
        with open(processed_lines_file, "w", encoding="utf-8") as f:
            json.dump(list(processed_lines), f, ensure_ascii=False)

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
