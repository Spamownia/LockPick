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
import requests
from collections import defaultdict
from ftplib import FTP
from io import BytesIO
import time
import os

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

# --- WCZYTANIE STANU ---
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        last_log_name = f.readline().strip()
        last_line_count = int(f.readline().strip())
else:
    last_log_name = ""
    last_line_count = 0

# --- GŁÓWNA PĘTLA ---
while True:
    print("[INFO] Sprawdzanie logów...")

    # --- POBIERANIE LISTY LOGÓW ---
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

    # --- POBRANIE NAJNOWSZEGO LOGU ---
    log_text = ""
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
    player_summary = {}

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

        # --- SUMARYCZNE DLA PODIUM ---
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

    with open("logi.csv", "a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(csv_rows)

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

    # --- TABELA ADMIN ---
    admin_csv_rows = [["Nick", "Rodzaj zamka", "Skuteczność", "Średni czas"]]
    last_nick_admin = None
    for (nick, lock_type), stats in sorted_data:
        if last_nick_admin and nick != last_nick_admin:
            admin_csv_rows.append([""] * 4)
        last_nick_admin = nick

        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        admin_csv_rows.append([nick, lock_type, f"{eff}%", f"{avg}s"])

    summary_block = "```\n"
    summary_block += f"{'Nick':<10} {'Zamek':<10} {'Skut.':<10} {'Śr. czas':<10}\n"
    summary_block += "-" * 45 + "\n"
    for row in admin_csv_rows[1:]:
        if any(row):
            summary_block += f"{row[0]:<10} {row[1]:<10} {row[2]:<10} {row[3]:<10}\n"
        else:
            summary_block += "\n"
    summary_block += "```"
    send_discord(summary_block, WEBHOOK_TABLE2)

    # --- TABELA PODIUM (SUMARYCZNE PER GRACZ) ---
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
        podium_block += f"{medals[i]} {i+1:<6} {nick:<10} {eff:<12} {avg:<10}\n"

    podium_block += "```"
    send_discord(podium_block, WEBHOOK_TABLE3)

    # --- ZAPIS STANU ---
    last_line_count = len(lines)
    with open(STATE_FILE, "w") as f:
        f.write(f"{latest_log}\n{last_line_count}\n")

    print("[INFO] Wysłano aktualizacje. Oczekiwanie 60s...")
    time.sleep(60)
