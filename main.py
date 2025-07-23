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

# --- ŁADOWANIE PRZETWORZONYCH LINII Z PLIKU ---
processed_lines_file = "processed_lines.json"
if os.path.isfile(processed_lines_file):
    with open(processed_lines_file, "r", encoding="utf-8") as f:
        processed_lines = set(json.load(f))
else:
    processed_lines = set()

# --- FUNKCJA GENEROWANIA TABEL ---
def generate_tables(data):
    # Admin table
    admin_header = (
        "Nick | Rodzaj zamka | Wszystkie podjęte próby | Udane | Nieudane | Skuteczność | Średni czas"
    )
    admin_lines = [admin_header]
    for (nick, lock_type), stats in sorted(data.items(), key=lambda x: (x[0][0], x[0][1])):
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        fail = stats["failed_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        line = f"{nick} | {lock_type} | {all_attempts} | {succ} | {fail} | {eff}% | {avg}s"
        admin_lines.append(line)
    admin_table = "```\n" + "\n".join(admin_lines) + "\n```"

    # Stats table
    stats_header = "Nick | Zamek | Skuteczność | Średni czas"
    stats_lines = [stats_header]
    for (nick, lock_type), stats in sorted(data.items(), key=lambda x: (x[0][0], x[0][1])):
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        line = f"{nick} | {lock_type} | {eff}% | {avg}s"
        stats_lines.append(line)
    stats_table = "```\n" + "\n".join(stats_lines) + "\n```"

    # Podium table
    medals = ["🥇", "🥈", "🥉"]
    user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})
    for (nick, _), stats in data.items():
        user_summary[nick]["success"] += stats["successful_attempts"]
        user_summary[nick]["total"] += stats["all_attempts"]
        user_summary[nick]["times"].extend(stats["times"])

    ranking = []
    for nick, summary in user_summary.items():
        total_attempts = summary["total"]
        total_success = summary["success"]
        times_all = summary["times"]

        eff = round(100 * total_success / total_attempts, 2) if total_attempts else 0
        avg = round(statistics.mean(times_all), 2) if times_all else 0

        ranking.append((nick, eff, avg))

    ranking = sorted(ranking, key=lambda x: (-x[1], x[2]))[:5]

    podium_lines = [""]
    podium_lines.append("Nick | Skuteczność | Średni czas")
    for i, (nick, eff, avg) in enumerate(ranking):
        medal = medals[i] if i < len(medals) else ""
        podium_lines.append(f"{medal} {nick} | {eff}% | {avg}s")

    podium_table = "```\n" + "\n".join(podium_lines) + "\n```"

    return admin_table, stats_table, podium_table

# --- FUNKCJA PRZETWARZANIA NOWYCH ZDARZEŃ ---
def process_new_events(new_events):
    # Wczytujemy aktualne dane z CSV, jeśli istnieje
    data = {}
    if os.path.isfile("logi.csv"):
        with open("logi.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                nick = row["Nick"]
                lock_type = row["Rodzaj zamka"]
                all_attempts = int(row["Wszystkie podjęte próby"])
                succ = int(row["Udane"])
                fail = int(row["Nieudane"])
                # średni czas i skuteczność zapiszemy do wyliczenia
                # ale średni czas potrzebujemy rozłożyć na czasy, więc tutaj przechowujemy dane do średniej
                # z uwagi na brak listy czasów, utworzymy nowe times dla nowych eventów i uśrednim w końcu

                # Zbudujemy times list jako pusta, rozbijanie nie jest możliwe
                data[(nick, lock_type)] = {
                    "all_attempts": all_attempts,
                    "successful_attempts": succ,
                    "failed_attempts": fail,
                    "times": [],  # uzupełnimy niżej
                }

    # Dodajemy nowe eventy do danych
    for entry in new_events:
        match = pattern.search(entry)
        if match:
            nick = match.group("nick")
            lock_type = match.group("lock_type")
            success = match.group("success")
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

    # Po dodaniu nowych eventów, wyliczamy średni czas uwzględniając stare dane + nowe
    # Niestety nie mamy dokładnych starych czasów, więc średni czas liczymy na podstawie nowych tylko
    # Uprościmy, licząc średni czas na podstawie tylko nowych eventów (szczególnie po starcie)
    # W praktyce to może powodować błędy, ale brak danych na więcej.

    # Zapis do CSV (nadpisanie)
    with open("logi.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Nick", "Rodzaj zamka", "Wszystkie podjęte próby", "Udane",
            "Nieudane", "Skuteczność", "Średni czas"
        ])
        for (nick, lock_type), stats in sorted(data.items(), key=lambda x: (x[0][0], x[0][1])):
            all_attempts = stats["all_attempts"]
            succ = stats["successful_attempts"]
            fail = stats["failed_attempts"]
            avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
            eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
            writer.writerow([nick, lock_type, all_attempts, succ, fail, f"{eff}%", f"{avg}s"])

    admin_table, stats_table, podium_table = generate_tables(data)
    send_discord(admin_table, WEBHOOK_TABLE1)
    send_discord(stats_table, WEBHOOK_TABLE2)
    send_discord(podium_table, WEBHOOK_TABLE3)

    print("[INFO] Wysłano wszystkie tabele.")

    # Zapisz przetworzone linie
    with open(processed_lines_file, "w", encoding="utf-8") as f:
        json.dump(list(processed_lines), f, ensure_ascii=False)

# --- FUNKCJA POCZĄTKOWEGO WGRANIA WSZYSTKICH LOGÓW ---
def initial_load_all_logs():
    global processed_lines

    print("[DEBUG] Początkowe pobieranie wszystkich logów z FTP...")

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

    all_new_events = []

    for log_file in log_files:
        print(f"[INFO] Przetwarzanie logu: {log_file}")
        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {log_file}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

        for line in log_text.splitlines():
            if line not in processed_lines:
                processed_lines.add(line)
                all_new_events.append(line)

    ftp.quit()

    if not all_new_events:
        print("[INFO] Brak nowych zdarzeń w logach.")
        return

    process_new_events(all_new_events)

# --- FUNKCJA SPRAWDZANIA NAJNOWSZEGO LOGU I NOWYCH LINII ---
def process_logs():
    global processed_lines

    print("[DEBUG] Sprawdzam nowe wpisy w najnowszym logu...")

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
    print(f"[INFO] Sprawdzanie nowego logu: {latest_log}")

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

    process_new_events(new_events)

# --- PĘTLA GŁÓWNA ---
def main_loop():
    initial_load_all_logs()

    while True:
        process_logs()
        time.sleep(60)

# --- START ---
if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
