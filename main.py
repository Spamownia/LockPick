Historyczne

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

# --- IMPORTY ---
import re
import csv
import statistics
from collections import defaultdict
from ftplib import FTP
from io import BytesIO
import os

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

# --- FUNKCJA GŁÓWNA ---
def import_all_logs():
    print("[DEBUG] Rozpoczynam import WSZYSTKICH logów...")

    # --- Wczytanie danych historycznych ---
    history_data = {}
    if os.path.isfile("logi.csv"):
        with open("logi.csv", newline='', encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # nagłówki
            for row in reader:
                if not any(row): continue
                nick, lock_type = row[0], row[1]
                all_attempts = int(row[2])
                successful_attempts = int(row[3])
                failed_attempts = int(row[4])
                avg_time = float(row[6].replace("s",""))

                history_data[(nick, lock_type)] = {
                    "all_attempts": all_attempts,
                    "successful_attempts": successful_attempts,
                    "failed_attempts": failed_attempts,
                    "times": [avg_time]*all_attempts  # przybliżenie
                }
                print(f"[DEBUG] Wczytano z CSV: {nick}, {lock_type}, próby: {all_attempts}, udane: {successful_attempts}")

    # --- Połączenie FTP ---
    ftp = FTP()
    ftp.connect(FTP_IP, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)

    log_files = []
    ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
    log_files = [f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")]
    log_files.sort()

    current_data = {}

    for log_file in log_files:
        print(f"[INFO] Przetwarzanie: {log_file}")

        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {log_file}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

        for match in pattern.finditer(log_text):
            nick = match.group("nick")
            lock_type = match.group("lock_type")
            success = match.group("success")
            failed_attempts = int(match.group("failed_attempts"))
            elapsed = float(match.group("elapsed"))

            print(f"[DEBUG] Log {log_file}: {nick}, {lock_type}, success={success}, elapsed={elapsed}")

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

    ftp.quit()
    print("[DEBUG] Pobieranie logów zakończone.")

    # --- SUMOWANIE historycznych + aktualnych ---
    final_data = {}
    for key in set(history_data.keys()).union(current_data.keys()):
        h = history_data.get(key, {"all_attempts":0,"successful_attempts":0,"failed_attempts":0,"times":[]})
        c = current_data.get(key, {"all_attempts":0,"successful_attempts":0,"failed_attempts":0,"times":[]})

        merged = {
            "all_attempts": h["all_attempts"] + c["all_attempts"],
            "successful_attempts": h["successful_attempts"] + c["successful_attempts"],
            "failed_attempts": h["failed_attempts"] + c["failed_attempts"],
            "times": h["times"] + c["times"],
        }

        final_data[key] = merged

    # --- ZAPIS CSV ---
    sorted_data = sorted(final_data.items(), key=lambda x: (x[0][0], lock_order.get(x[0][1],99)))
    csv_rows = []

    for (nick, lock_type), stats in sorted_data:
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        fail = stats["failed_attempts"]
        avg = round(statistics.mean(stats["times"]),2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts,2) if all_attempts else 0

        csv_rows.append([nick, lock_type, all_attempts, succ, fail, f"{eff}%", f"{avg}s"])
        print(f"[DEBUG] Zapis: {nick}, {lock_type}, próby: {all_attempts}, udane: {succ}, nieudane: {fail}, skuteczność: {eff}%, avg: {avg}s")

    with open("logi.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Nick","Rodzaj zamka","Ilość wszystkich prób","Ilość udanych prób","Ilość nieudanych prób","Skuteczność","Śr. czas"])
        writer.writerows(csv_rows)

    print("[INFO] Import zakończony – plik logi.csv zaktualizowany.")

# --- START ---
if __name__ == "__main__":
    import_all_logs()
