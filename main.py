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
import base64
import os
import time
import threading
from collections import defaultdict
from ftplib import FTP
from io import BytesIO
from flask import Flask

# --- FLASK SETUP ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- FUNKCJA WYSYŁANIA NA DISCORD ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

def send_discord(content):
    print("[DEBUG] Wysyłanie na webhook...")
    requests.post(WEBHOOK_URL, json={"content": content})

# --- KONFIGURACJA FTP ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

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

# --- FUNKCJA SPRAWDZANIA NOWYCH WPISÓW ---
seen_lines = []

def process_new_entries():
    global seen_lines
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
        return

    latest_log = log_files[-1]
    print(f"[INFO] Najnowszy log: {latest_log}")

    with BytesIO() as bio:
        ftp.retrbinary(f"RETR {latest_log}", bio.write)
        log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

    ftp.quit()

    new_lines = [line for line in log_text.splitlines() if line not in seen_lines]
    if not new_lines:
        print("[INFO] Brak nowych linii.")
        return

    print(f"[INFO] Znaleziono {len(new_lines)} nowych linii.")

    data = {}
    user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})

    for line in new_lines:
        match = pattern.search(line)
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

    # --- TABELA GŁÓWNA ---
    sorted_data = sorted(
        data.items(),
        key=lambda x: (x[0][0], lock_order.get(x[0][1], 99))
    )
    table_block = "```\n"
    table_block += f"{'Nick':<10} {'Zamek':<10} {'Wszystkie':<12} {'Udane':<6} {'Nieudane':<9} {'Skut.':<8} {'Śr. czas':<8}\n"
    table_block += "-" * 70 + "\n"
    for (nick, lock_type), stats in sorted_data:
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        fail = stats["failed_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        table_block += f"{nick:<10} {lock_type:<10} {all_attempts:<12} {succ:<6} {fail:<9} {eff}%{'':<3} {avg}s\n"
    table_block += "```"
    send_discord(table_block)
    print("[INFO] Wysłano tabelę główną.")

    # --- TABELA ADMIN ---
    admin_block = "```\n"
    admin_block += f"{'Nick':<10} {'Zamek':<10} {'Skut.':<10} {'Śr. czas':<10}\n"
    admin_block += "-" * 45 + "\n"
    for (nick, lock_type), stats in sorted_data:
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        admin_block += f"{nick:<10} {lock_type:<10} {eff}%{'':<6} {avg}s{'':<4}\n"
    admin_block += "```"
    send_discord(admin_block)
    print("[INFO] Wysłano tabelę admin.")

    # --- TABELA PODIUM ---
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
    send_discord(podium_block)
    print("[INFO] Wysłano tabelę podium.")

    seen_lines += new_lines

# --- PĘTLA W WĄTKU ---
def loop():
    while True:
        process_new_entries()
        time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
