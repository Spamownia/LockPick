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
    print(f"[DEBUG] Wysyłanie na Discord ({webhook_url}):\n{content[:500]}")  # ogranicz do 500 znaków w logu
    requests.post(webhook_url, json={"content": content})

# --- KONFIGURACJA FTP ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

# --- WEBHOOKI ---
WEBHOOK_TABLE1 = "https://discord.com/api/webhooks/..."
WEBHOOK_TABLE2 = "https://discord.com/api/webhooks/..."
WEBHOOK_TABLE3 = "https://discord.com/api/webhooks/..."

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
    try:
        ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
    except Exception as e:
        print(f"[ERROR] Nie udało się pobrać listy plików: {e}")

    log_files = [f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików gameplay_*.log")
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

    log_lines = log_text.splitlines()
    print(f"[DEBUG] Log zawiera {len(log_lines)} linii.")

    # --- Wczytywanie ostatnio przetworzonej linii ---
    last_line_file = "last_processed_line.txt"
    last_processed_line = None
    if os.path.isfile(last_line_file):
        with open(last_line_file, "r", encoding="utf-8") as f:
            last_processed_line = f.read().strip()
        print(f"[DEBUG] Ostatnio przetworzona linia: {last_processed_line[:200]}")

    # --- Znajdowanie pozycji ostatniej przetworzonej linii ---
    start_index = 0
    if last_processed_line:
        for i, line in enumerate(log_lines):
            if last_processed_line in line:
                start_index = i + 1
                break
    print(f"[DEBUG] Przetwarzanie od indeksu {start_index}.")

    # --- Wyodrębnianie tylko nowych linii ---
    new_lines = log_lines[start_index:]
    print(f"[DEBUG] Znaleziono {len(new_lines)} nowych linii do przetworzenia.")
    if not new_lines:
        print("[INFO] Brak nowych zdarzeń. Nie wysyłam tabel.")
        return

    # --- Parsowanie tylko nowych linii ---
    current_data = {}
    user_lock_times = defaultdict(lambda: defaultdict(list))
    for line in new_lines:
        match = pattern.search(line)
        if not match:
            continue

        nick = match.group("nick")
        lock_type = match.group("lock_type")
        success = match.group("success")
        failed_attempts = int(match.group("failed_attempts"))
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
        user_lock_times[nick][lock_type].append(elapsed)

    if not current_data:
        print("[INFO] Brak nowych zdarzeń parsowanych regexem. Nie wysyłam tabel.")
        return

    print(f"[INFO] Nowe zdarzenia: {sum(d['all_attempts'] for d in current_data.values())}")

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

    # --- Sumowanie danych ---
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

    # --- Sortowanie danych do tabel ---
    sorted_data = sorted(combined_data.items(), key=lambda x: (x[0][0], lock_order.get(x[0][1], 99)))

    # --- Tabela główna ---
    csv_rows = []
    for (nick, lock_type), stats in sorted_data:
        avg_time = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        effectiveness = round(100 * stats["successful_attempts"] / stats["all_attempts"], 2) if stats["all_attempts"] else 0

        csv_rows.append([
            nick, lock_type, stats["all_attempts"], stats["successful_attempts"], stats["failed_attempts"],
            f"{effectiveness}%", f"{avg_time}s"
        ])

    with open("logi.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Nick", "Rodzaj zamka", "Ilość wszystkich prób", "Ilość udanych prób",
            "Ilość nieudanych prób", "Skuteczność", "Śr. czas"
        ])
        writer.writerows(csv_rows)
    print("[DEBUG] Zapisano plik logi.csv.")

    # --- Tabela admin ---
    admin_rows = [["Nick", "Rodzaj zamka", "Skuteczność", "Średni czas"]]
    for (nick, lock_type), stats in sorted_data:
        avg_time = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        effectiveness = round(100 * stats["successful_attempts"] / stats["all_attempts"], 2) if stats["all_attempts"] else 0
        admin_rows.append([nick, lock_type, f"{effectiveness}%", f"{avg_time}s"])

    with open("logi_admin.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(admin_rows)
    print("[DEBUG] Zapisano plik logi_admin.csv.")

    # --- Podium ---
    ranking = []
    for nick in user_lock_times:
        times_all = [t for lock in user_lock_times[nick].values() for t in lock]
        total_attempts = len(times_all)
        total_success = sum(1 for lock in user_lock_times[nick].values() for _ in lock)
        effectiveness = round(100 * total_success / total_attempts, 2) if total_attempts else 0
        avg_time = round(statistics.mean(times_all), 2) if total_attempts else 0
        ranking.append((nick, effectiveness, avg_time))

    ranking = sorted(ranking, key=lambda x: (-x[1], x[2]))[:5]

    # --- Wysyłka tabel ---
    table1 = "Tabela główna\n```\n" + "\n".join([str(r) for r in csv_rows]) + "\n```"
    send_discord(table1, WEBHOOK_TABLE1)

    table2 = "Tabela admin\n```\n" + "\n".join([str(r) for r in admin_rows]) + "\n```"
    send_discord(table2, WEBHOOK_TABLE2)

    podium = "🏆 Podium\n```\n" + "\n".join([f"{i+1}. {nick} {eff}% {avg}s" for i,(nick,eff,avg) in enumerate(ranking)]) + "\n```"
    send_discord(podium, WEBHOOK_TABLE3)

    # --- Zapisanie ostatniej przetworzonej linii ---
    if new_lines:
        with open(last_line_file, "w", encoding="utf-8") as f:
            f.write(new_lines[-1])
        print(f"[DEBUG] Zapisano ostatnią przetworzoną linię.")

    print("[INFO] Zakończono przetwarzanie i wysyłkę.")

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
