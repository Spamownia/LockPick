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
    requests.post(webhook_url, json={"content": content})

# --- KONFIGURACJA FTP ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

# --- WEBHOOKI ---
WEBHOOK_TABLE1 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
WEBHOOK_TABLE2 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
WEBHOOK_TABLE3 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

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

# --- FUNKCJA TWORZENIA PLIKÓW JEŚLI BRAK ---
def create_if_missing(filename, headers):
    if not os.path.isfile(filename):
        with open(filename, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
        print(f"[INFO] Utworzono brakujący plik {filename}.")

# --- GLOBALNE ---
last_processed_lines = set()

# --- GŁÓWNA FUNKCJA PRZETWARZAJĄCA LOGI ---
def process_logs():
    global last_processed_lines
    print("[DEBUG] Rozpoczynam przetwarzanie logów...")

    ftp = FTP()
    ftp.connect(FTP_IP, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)

    log_files = []
    try:
        ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
    except Exception as e:
        print(f"[ERROR] Nie udało się pobrać listy plików: {e}")

    log_files = [f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")]
    if not log_files:
        print("[ERROR] Brak plików gameplay_*.log na FTP.")
        ftp.quit()
        return

    latest_log = sorted(log_files)[-1]
    print(f"[INFO] Najnowszy log: {latest_log}")

    with BytesIO() as bio:
        ftp.retrbinary(f"RETR {latest_log}", bio.write)
        log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

    ftp.quit()

    # --- SPRAWDZENIE NOWYCH LINII ---
    current_lines = set(pattern.findall(log_text))
    new_lines = current_lines - last_processed_lines

    if not new_lines:
        print("[INFO] Brak nowych wpisów w logu.")
        return
    else:
        print(f"[INFO] Wykryto {len(new_lines)} nowych wpisów.")
        last_processed_lines = current_lines

    # --- PARSOWANIE ---
    data = {}
    player_summary = {}

    for match in pattern.finditer(log_text):
        nick = match.group("nick")
        lock_type = match.group("lock_type")
        success = match.group("success")
        failed_attempts = int(match.group("failed_attempts"))
        elapsed = float(match.group("elapsed"))

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

        if nick not in player_summary:
            player_summary[nick] = {
                "all_attempts": 0,
                "successful_attempts": 0,
                "times": []
            }
        player_summary[nick]["all_attempts"] += 1
        if success == "Yes":
            player_summary[nick]["successful_attempts"] += 1
        player_summary[nick]["times"].append(elapsed)

    # --- GENEROWANIE I WYSYŁKA TRZECH TABEL ---
    # Tabela główna
    sorted_data = sorted(
        data.items(),
        key=lambda x: (x[0][0], lock_order.get(x[0][1], 99))
    )
    csv_rows = []
    last_nick = None
    for (nick, lock_type), stats in sorted_data:
        if last_nick and nick != last_nick:
            csv_rows.append([""] * 7)
        last_nick = nick
        all_attempts = stats["all_attempts"]
        successful_attempts = stats["successful_attempts"]
        failed_attempts = stats["failed_attempts"]
        avg_time = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        effectiveness = round(100 * successful_attempts / all_attempts, 2) if all_attempts else 0
        csv_rows.append([
            nick, lock_type, all_attempts, successful_attempts, failed_attempts,
            f"{effectiveness}%", f"{avg_time}s"
        ])

    # Wysyłka tabeli głównej
    table_block = "```\n"
    table_block += f"{'Nick':<10} {'Zamek':<10} {'Wszystkie':<12} {'Udane':<6} {'Nieudane':<9} {'Skut.':<8} {'Śr. czas':<8}\n"
    table_block += "-" * 70 + "\n"
    for row in csv_rows:
        if any(row):
            table_block += f"{row[0]:<10} {row[1]:<10} {str(row[2]):<12} {str(row[3]):<6} {str(row[4]):<9} {row[5]:<8} {row[6]:<8}\n"
        else:
            table_block += "\n"
    table_block += "```"
    send_discord(table_block, WEBHOOK_TABLE1)

    # Tabela admin
    admin_block = "```\n"
    admin_block += f"{'Nick':<10} {'Zamek':<10} {'Skut.':<10} {'Śr. czas':<10}\n"
    admin_block += "-" * 45 + "\n"
    for (nick, lock_type), stats in sorted_data:
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        admin_block += f"{nick:<10} {lock_type:<10} {str(eff)+'%':<10} {str(avg)+'s':<10}\n"
    admin_block += "```"
    send_discord(admin_block, WEBHOOK_TABLE2)

    # Podium
    ranking = []
    for nick in player_summary:
        times_all = player_summary[nick]["times"]
        total_attempts = player_summary[nick]["all_attempts"]
        total_success = player_summary[nick]["successful_attempts"]
        effectiveness = round(100 * total_success / total_attempts, 2) if total_attempts else 0
        avg_time = round(statistics.mean(times_all), 2) if total_attempts else 0
        ranking.append((nick, effectiveness, avg_time))
    ranking = sorted(ranking, key=lambda x: (-x[1], x[2]))[:5]

    podium_block = "```\n"
    podium_block += "🏆 PODIUM\n"
    podium_block += "-" * 50 + "\n"
    podium_block += f"{'Miejsce':<8}{'Nick':<12}{'Skut.':<10}{'Śr. czas':<10}\n"
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    for i, (nick, eff, avg) in enumerate(ranking):
        medal = medals[i]
        podium_block += f"{medal:<2}{str(i+1):<6}{nick:<12}{str(eff)+'%':<10}{str(avg)+'s':<10}\n"
    podium_block += "```"
    send_discord(podium_block, WEBHOOK_TABLE3)

    print("[INFO] Zakończono przetwarzanie logu i wysyłkę.")

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
