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

# --- FUNKCJA GŁÓWNA ---
def process_logs():
    print("[DEBUG] Rozpoczynam przetwarzanie logów...")

    # --- Pobierz ostatnio przetworzony log ---
    last_log_file = "last_processed.txt"
    last_processed = ""
    if os.path.isfile(last_log_file):
        with open(last_log_file, "r") as f:
            last_processed = f.read().strip()
    print(f"[DEBUG] Ostatnio przetworzony log: {last_processed}")

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
    print(f"[INFO] Najnowszy log: {latest_log}")

    # --- Sprawdź, czy to nowy log ---
    if latest_log == last_processed:
        print("[INFO] Log został już przetworzony. Pomijam.")
        ftp.quit()
        return

    with BytesIO() as bio:
        ftp.retrbinary(f"RETR {latest_log}", bio.write)
        log_text = bio.getvalue().decode("utf-16-le", errors="ignore")
    ftp.quit()

    # --- Parsowanie logu ---
    data = {}
    user_lock_times = defaultdict(lambda: defaultdict(list))

    for match in pattern.finditer(log_text):
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
        user_lock_times[nick][lock_type].append(elapsed)

    print(f"[DEBUG] Znaleziono {len(data)} rekordów w logu.")

    if not data:
        print("[INFO] Brak nowych zdarzeń. Pomijam wysyłkę.")
        return

    # --- TABELA GŁÓWNA ---
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
        succ = stats["successful_attempts"]
        fail = stats["failed_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0

        csv_rows.append([
            nick, lock_type, all_attempts, succ, fail,
            f"{eff}%", f"{avg}s"
        ])

    # --- ZAPIS CSV (dopisywanie) ---
    file_exists = os.path.isfile("logi.csv")
    with open("logi.csv", "a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Nick", "Rodzaj zamka", "Ilość wszystkich prób", "Ilość udanych prób",
                "Ilość nieudanych prób", "Skuteczność", "Śr. czas"
            ])
        writer.writerows(csv_rows)

    print("[INFO] Zaktualizowano plik logi.csv.")

    # --- WYSYŁKA TABELI GŁÓWNEJ ---
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
        admin_block += f"{nick:<10} {lock_type:<10} {str(eff)+'%':<10} {str(avg)+'s':<10}\n"
    admin_block += "```"
    send_discord(admin_block, WEBHOOK_TABLE2)
    print("[INFO] Wysłano tabelę admin.")

    # --- TABELA PODIUM ---
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    ranking = []
    for nick in user_lock_times:
        times_all = [t for lock in user_lock_times[nick].values() for t in lock]
        total_attempts = len(times_all)
        total_success = sum(len(lock) for lock in user_lock_times[nick].values())
        eff = round(100 * total_success / total_attempts, 2) if total_attempts else 0
        avg = round(statistics.mean(times_all), 2) if total_attempts else 0
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

    # --- ZAPIS NAZWY OSTATNIEGO LOGU ---
    with open(last_log_file, "w") as f:
        f.write(latest_log)
    print(f"[DEBUG] Zapisano ostatnio przetworzony log: {latest_log}")

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
