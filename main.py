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
import os
import time
import threading
from collections import defaultdict
from ftplib import FTP
from io import BytesIO
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

# --- WEBHOOK ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

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
def process_loop():
    seen_lines = set()

    while True:
        print("[DEBUG] Sprawdzam nowe linie logu...")

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
            time.sleep(60)
            continue

        latest_log = log_files[-1]
        print(f"[INFO] Najnowszy log: {latest_log}")

        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {latest_log}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

        ftp.quit()

        new_lines = [line for line in log_text.splitlines() if line not in seen_lines]
        if not new_lines:
            print("[INFO] Brak nowych linii.")
            time.sleep(60)
            continue

        print(f"[INFO] Znaleziono {len(new_lines)} nowych linii.")

        # --- Wczytanie dotychczasowych danych z pliku ---
        data_dict = {}
        if os.path.exists("logi.csv"):
            with open("logi.csv", "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)  # nagłówek
                for row in reader:
                    if len(row) < 7: continue
                    nick, lock_type, all_attempts, succ, fail, eff, avg = row
                    key = (nick, lock_type)
                    data_dict[key] = {
                        "all_attempts": int(all_attempts),
                        "successful_attempts": int(succ),
                        "failed_attempts": int(fail),
                        "times": [float(avg.rstrip('s'))] * int(all_attempts)
                    }

        user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})

        # --- Przetwarzanie nowych linii ---
        for line in new_lines:
            match = pattern.search(line)
            if match:
                nick = match.group("nick")
                lock_type = match.group("lock_type")
                success = match.group("success")
                elapsed = float(match.group("elapsed"))

                key = (nick, lock_type)
                if key not in data_dict:
                    data_dict[key] = {
                        "all_attempts": 0,
                        "successful_attempts": 0,
                        "failed_attempts": 0,
                        "times": []
                    }

                data_dict[key]["all_attempts"] += 1
                if success == "Yes":
                    data_dict[key]["successful_attempts"] += 1
                else:
                    data_dict[key]["failed_attempts"] += 1
                data_dict[key]["times"].append(elapsed)

                user_summary[nick]["total"] += 1
                user_summary[nick]["times"].append(elapsed)
                if success == "Yes":
                    user_summary[nick]["success"] += 1

        # --- Zapis do pliku CSV (nadpisanie pełnym zestawem) ---
        with open("logi.csv", "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Nick", "Rodzaj zamka", "Ilość wszystkich prób", "Ilość udanych prób",
                             "Ilość nieudanych prób", "Skuteczność", "Śr. czas"])
            for (nick, lock_type), stats in data_dict.items():
                all_attempts = stats["all_attempts"]
                succ = stats["successful_attempts"]
                fail = stats["failed_attempts"]
                avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
                eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
                writer.writerow([nick, lock_type, all_attempts, succ, fail, f"{eff}%", f"{avg}s"])

        # --- Generowanie tabeli głównej ---
        table_block = "```\n"
        table_block += f"{'Nick':<12} {'Zamek':<10} {'Wszystkie':<10} {'Udane':<6} {'Nieudane':<9} {'Skut.':<6} {'Śr. czas':<8}\n"
        table_block += "-" * 70 + "\n"
        for (nick, lock_type), stats in data_dict.items():
            all_attempts = stats["all_attempts"]
            succ = stats["successful_attempts"]
            fail = stats["failed_attempts"]
            avg = round(statistics.mean(stats["times"]), 2)
            eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
            table_block += f"{nick:<12} {lock_type:<10} {all_attempts:<10} {succ:<6} {fail:<9} {eff}%   {avg}s\n"
        table_block += "```"

        # --- Generowanie tabeli admin ---
        admin_block = "```\n"
        admin_block += f"{'Nick':<12} {'Zamek':<10} {'Skut.':<6} {'Śr. czas':<8}\n"
        admin_block += "-" * 40 + "\n"
        for (nick, lock_type), stats in data_dict.items():
            all_attempts = stats["all_attempts"]
            succ = stats["successful_attempts"]
            eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
            avg = round(statistics.mean(stats["times"]), 2)
            admin_block += f"{nick:<12} {lock_type:<10} {eff}%   {avg}s\n"
        admin_block += "```"

        # --- Generowanie tabeli podium ---
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

        podium_block = "```\n"
        podium_block += f"{' ':<2}{'Nick':<12}{'Skut.':<8}{'Śr. czas':<8}\n"
        podium_block += "-" * 30 + "\n"
        for i, (nick, eff, avg) in enumerate(ranking):
            medal = medals[i]
            podium_block += f"{medal:<2}{nick:<12}{eff}%   {avg}s\n"
        podium_block += "```"

        # --- Wysyłka wszystkich tabel ---
        send_discord(table_block, WEBHOOK_URL)
        send_discord(admin_block, WEBHOOK_URL)
        send_discord(podium_block, WEBHOOK_URL)

        seen_lines.update(new_lines)
        time.sleep(60)

# --- START ---
if __name__ == "__main__":
    threading.Thread(target=process_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
