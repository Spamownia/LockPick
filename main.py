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
from flask import Flask

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

STATE_FILE = "last_log_state.txt"

# --- WEBHOOKI ---
WEBHOOK_TABLE1 = "https://discord.com/api/webhooks/WEBHOOK_TABLE1"
WEBHOOK_TABLE2 = "https://discord.com/api/webhooks/WEBHOOK_TABLE2"
WEBHOOK_TABLE3 = "https://discord.com/api/webhooks/WEBHOOK_TABLE3"

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

# --- GŁÓWNA FUNKCJA W PĘTLI ---
def main_loop():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            last_log_name = f.readline().strip()
            last_line_count = int(f.readline().strip())
    else:
        last_log_name = ""
        last_line_count = 0

    while True:
        print("[INFO] Sprawdzanie logów...")

        # --- POBIERANIE LOGÓW Z FTP ---
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
            time.sleep(60)
            continue

        latest_log = sorted(log_files)[-1]

        # --- POBRANIE LOGU ---
        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {latest_log}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

        ftp.quit()

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

        # --- PARSOWANIE NOWYCH LINII ---
        new_log_text = "\n".join(new_lines)

        data = {}
        user_lock_times = defaultdict(lambda: defaultdict(list))

        for match in pattern.finditer(new_log_text):
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
            user_lock_times[nick][lock_type].append(elapsed)

        sorted_data = sorted(
            data.items(),
            key=lambda x: (x[0][0], lock_order.get(x[0][1], 99))
        )

        # --- TABELA GŁÓWNA ---
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

        with open("logi.csv", "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Nick", "Rodzaj zamka", "Ilość wszystkich prób", "Ilość udanych prób",
                "Ilość nieudanych prób", "Skuteczność", "Śr. czas"
            ])
            writer.writerows(csv_rows)

        # --- WYSYŁKA TABEL ---
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

        # --- ZAPIS STANU ---
        last_line_count = len(lines)
        with open(STATE_FILE, "w") as f:
            f.write(f"{latest_log}\n{last_line_count}\n")

        print(f"[INFO] Wysłano aktualizacje. Oczekiwanie 60s...")
        time.sleep(60)

# 🔷 Flask server setup
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    # Uruchom główną pętlę w osobnym wątku
    t = threading.Thread(target=main_loop, daemon=True)
    t.start()

    # Uruchom serwer Flask na 0.0.0.0:10000
    app.run(host='0.0.0.0', port=10000)
