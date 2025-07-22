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
import time
import os
import threading
import traceback
from flask import Flask

# --- KONFIGURACJA SERWERA FLASK ---
app = Flask(__name__)
@app.route('/')
def index():
    return "Alive"

# --- FUNKCJA WYSYŁANIA NA DISCORD ---
def send_discord(content, webhook_url):
    requests.post(
        webhook_url,
        json={"content": content}
    )

# --- KONFIGURACJA FTP ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

# --- PLIKI STANU I CSV ---
STATE_FILE = "last_log_state.txt"
CSV_FILE = "logi.csv"

# --- WEBHOOKI ---
WEBHOOK_TABLE1 = "https://discord.com/api/webhooks/..."
WEBHOOK_TABLE2 = "https://discord.com/api/webhooks/..."
WEBHOOK_TABLE3 = "https://discord.com/api/webhooks/..."

# --- WZORZEC PARSERA ---
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

# --- FUNKCJA GŁÓWNA PĘTLI ---
def main_loop():
    # --- TWORZENIE PLIKÓW JEŚLI NIE ISTNIEJĄ ---
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Nick", "Rodzaj zamka", "Success", "Nieudane próby", "Czas"])
        print(f"[INFO] Utworzono plik {CSV_FILE}.")

    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w") as f:
            f.write("\n0\n")
        print(f"[INFO] Utworzono plik {STATE_FILE}.")

    # --- WCZYTANIE STANU ---
    with open(STATE_FILE, "r") as f:
        last_log_name = f.readline().strip()
        last_line_count = int(f.readline().strip())

    while True:
        print("[INFO] Sprawdzanie logów...")

        # --- POBIERANIE LISTY LOGÓW ---
        try:
            ftp = FTP()
            ftp.connect(FTP_IP, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_PATH)
            print("[DEBUG] Połączono z FTP.")

            log_files = []
            ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
        except Exception as e:
            print(f"[ERROR] FTP: {e}")
            traceback.print_exc()
            time.sleep(60)
            continue

        log_files = [f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")]
        if not log_files:
            print("[ERROR] Brak plików gameplay_*.log na FTP.")
            time.sleep(60)
            continue

        latest_log = sorted(log_files)[-1]

        # --- POBRANIE NAJNOWSZEGO LOGU ---
        try:
            with BytesIO() as bio:
                ftp.retrbinary(f"RETR {latest_log}", bio.write)
                log_text = bio.getvalue().decode("utf-16-le", errors="ignore")
            ftp.quit()
            print(f"[DEBUG] Pobrano plik {latest_log}.")
        except Exception as e:
            print(f"[ERROR] Pobieranie logu: {e}")
            traceback.print_exc()
            time.sleep(60)
            continue

        lines = log_text.splitlines()

        # --- SPRAWDZANIE NOWYCH LINII ---
        if latest_log == last_log_name:
            new_lines = lines[last_line_count:]
        else:
            new_lines = lines
            last_log_name = latest_log
            last_line_count = 0

        if not new_lines:
            print("[INFO] Brak nowych zdarzeń w logu.")
            time.sleep(60)
            continue

        print(f"[DEBUG] Nowych linii: {len(new_lines)}")

        # --- PARSOWANIE NOWYCH LINII I DODAWANIE DO CSV ---
        parsed_rows = []
        for match in pattern.finditer("\n".join(new_lines)):
            nick = match.group("nick")
            lock_type = match.group("lock_type")
            success = match.group("success")
            failed_attempts = int(match.group("failed_attempts"))
            elapsed = float(match.group("elapsed"))
            parsed_rows.append([nick, lock_type, success, failed_attempts, elapsed])

        with open(CSV_FILE, "a", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(parsed_rows)
        print(f"[INFO] Dodano {len(parsed_rows)} nowych wpisów do {CSV_FILE}.")

        # --- WCZYTANIE CAŁEGO CSV DO GENEROWANIA TABEL ---
        all_data = []
        with open(CSV_FILE, "r", newline='', encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # pomiń nagłówek
            for row in reader:
                if row:
                    all_data.append(row)

        # --- PRZETWARZANIE PEŁNYCH STATYSTYK ---
        data = {}
        player_summary = {}
        for row in all_data:
            nick, lock_type, success, failed_attempts, elapsed = row
            failed_attempts = int(failed_attempts)
            elapsed = float(elapsed)
            key = (nick, lock_type)
            if key not in data:
                data[key] = {"all_attempts": 0, "successful_attempts": 0, "failed_attempts": 0, "times": []}
            data[key]["all_attempts"] += 1
            if success == "Yes":
                data[key]["successful_attempts"] += 1
            else:
                data[key]["failed_attempts"] += 1
            data[key]["times"].append(elapsed)

            if nick not in player_summary:
                player_summary[nick] = {"all_attempts": 0, "successful_attempts": 0, "times": []}
            player_summary[nick]["all_attempts"] += 1
            if success == "Yes":
                player_summary[nick]["successful_attempts"] += 1
            player_summary[nick]["times"].append(elapsed)

        # --- GENEROWANIE I WYSYŁKA TABEL ---
        send_all_tables(data, player_summary)

        # --- ZAPIS STANU ---
        last_line_count = len(lines)
        with open(STATE_FILE, "w") as f:
            f.write(f"{latest_log}\n{last_line_count}\n")

        print("[INFO] Wysłano zaktualizowane statystyki. Czekam 60 sekund...")
        time.sleep(60)

# --- FUNKCJA GENEROWANIA I WYSYŁKI TABEL ---
def send_all_tables(data, player_summary):
    # --- SORTOWANIE ---
    sorted_data = sorted(data.items(), key=lambda x: (x[0][0], lock_order.get(x[0][1], 99)))

    # --- TABELA GŁÓWNA (TABLE1) ---
    table_block = "```\n"
    table_block += f"{'Nick':<10} {'Zamek':<10} {'Wszystkie':<12} {'Udane':<6} {'Nieudane':<9} {'Skut.':<8} {'Śr. czas':<8}\n"
    table_block += "-" * 70 + "\n"
    for (nick, lock_type), stats in sorted_data:
        all_attempts = stats["all_attempts"]
        successful_attempts = stats["successful_attempts"]
        failed_attempts = stats["failed_attempts"]
        avg_time = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        effectiveness = round(100 * successful_attempts / all_attempts, 2) if all_attempts else 0
        table_block += f"{nick:<10} {lock_type:<10} {all_attempts:<12} {successful_attempts:<6} {failed_attempts:<9} {effectiveness:<8}% {avg_time:<8}s\n"
    table_block += "```"
    send_discord(table_block, WEBHOOK_TABLE1)

    # --- TABELA ADMIN (TABLE2) ---
    admin_block = "```\n"
    admin_block += f"{'Nick':<10} {'Zamek':<10} {'Skut.':<10} {'Śr. czas':<10}\n"
    admin_block += "-" * 45 + "\n"
    for (nick, lock_type), stats in sorted_data:
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        admin_block += f"{nick:<10} {lock_type:<10} {eff:<10}% {avg:<10}s\n"
    admin_block += "```"
    send_discord(admin_block, WEBHOOK_TABLE2)

    # --- PODIUM (TABLE3) ---
    podium_block = "```\n"
    podium_block += "           🏆 PODIUM           \n"
    podium_block += "--------------------------------\n"
    podium_block += f"{'Miejsce':<8} {'Nick':<10} {'Skuteczność':<12} {'Średni czas':<10}\n"
    podium = []
    for nick, stats in player_summary.items():
        all_attempts = stats["all_attempts"]
        successful_attempts = stats["successful_attempts"]
        eff = round(100 * successful_attempts / all_attempts, 2) if all_attempts else 0
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        podium.append((nick, eff, avg))
    podium = sorted(podium, key=lambda x: (-x[1], x[2]))[:5]
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    for i, (nick, eff, avg) in enumerate(podium):
        podium_block += f"{medals[i]} {i+1:<6} {nick:<10} {eff:<12}% {avg:<10}s\n"
    podium_block += "```"
    send_discord(podium_block, WEBHOOK_TABLE3)

# --- START SERWERA I PĘTLI ---
if __name__ == "__main__":
    threading.Thread(target=main_loop).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
