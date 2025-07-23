# --- AUTOMATYCZNA INSTALACJA (cicho) ---
import subprocess
import sys
import os

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
import threading
import time
import json
from flask import Flask

# --- ŚCIEŻKI PLIKÓW ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "logi.csv")
PROCESSED_FILE = os.path.join(BASE_DIR, "processed_lines.json")

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

# --- ŁADOWANIE PRZETWORZONYCH LINII Z PLIKU ---
if os.path.isfile(PROCESSED_FILE):
    with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
        processed_lines = set(json.load(f))
else:
    processed_lines = set()

# --- FUNKCJE GENEROWANIA TABEL ---

def generate_tables(data):
    # data: dict with keys (nick, lock_type) and values dict with stats

    # --- Admin Table ---
    admin_rows = []
    for (nick, lock_type), stats in sorted(data.items(), key=lambda x: (x[0][0].lower(), x[0][1].lower())):
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        fail = stats["failed_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        admin_rows.append({
            "Nick": nick,
            "Rodzaj zamka": lock_type,
            "Wszystkie podjęte próby": all_attempts,
            "Udane": succ,
            "Nieudane": fail,
            "Skuteczność": f"{eff}%",
            "Średni czas": f"{avg}s"
        })

    admin_table = "```\nNick | Rodzaj zamka | Wszystkie podjęte próby | Udane | Nieudane | Skuteczność | Średni czas\n"
    admin_table += "-" * 80 + "\n"
    for row in admin_rows:
        line = f"{row['Nick']} | {row['Rodzaj zamka']} | {row['Wszystkie podjęte próby']} | {row['Udane']} | {row['Nieudane']} | {row['Skuteczność']} | {row['Średni czas']}"
        admin_table += line + "\n"
    admin_table += "```"

    # --- Statystyki Table ---
    stats_rows = []
    for (nick, lock_type), stats in sorted(data.items(), key=lambda x: (x[0][0].lower(), x[0][1].lower())):
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        stats_rows.append({
            "Nick": nick,
            "Zamek": lock_type,
            "Skuteczność": f"{eff}%",
            "Średni czas": f"{avg}s"
        })

    stats_table = "```\nNick | Zamek | Skuteczność | Średni czas\n"
    stats_table += "-" * 40 + "\n"
    for row in stats_rows:
        line = f"{row['Nick']} | {row['Zamek']} | {row['Skuteczność']} | {row['Średni czas']}"
        stats_table += line + "\n"
    stats_table += "```"

    # --- Podium Table ---
    # Suma skuteczności i średnich czasów per nick (sumujemy średni czas * próby do średniej ważonej)
    summary = defaultdict(lambda: {"total_attempts": 0, "successes": 0, "time_weighted_sum": 0})

    for (nick, lock_type), stats in data.items():
        attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        avg_time = statistics.mean(stats["times"]) if stats["times"] else 0
        summary[nick]["total_attempts"] += attempts
        summary[nick]["successes"] += succ
        summary[nick]["time_weighted_sum"] += avg_time * attempts

    podium_list = []
    for nick, vals in summary.items():
        if vals["total_attempts"] == 0:
            eff = 0
            avg = 0
        else:
            eff = round(100 * vals["successes"] / vals["total_attempts"], 2)
            avg = round(vals["time_weighted_sum"] / vals["total_attempts"], 2)
        podium_list.append((nick, eff, avg))

    podium_list.sort(key=lambda x: (-x[1], x[2]))
    medals = ["🥇", "🥈", "🥉"]

    podium_table = "```\n   Nick | Skuteczność | Średni czas\n"
    podium_table += "-" * 38 + "\n"
    for i, (nick, eff, avg) in enumerate(podium_list):
        medal = medals[i] if i < 3 else "   "
        line = f"{medal} {nick} | {eff}% | {avg}s"
        podium_table += line + "\n"
    podium_table += "```"

    return admin_table, stats_table, podium_table

# --- FUNKCJA PROCESOWANIA LOGÓW ---

def process_logs():
    global processed_lines

    print("[DEBUG] Rozpoczynam przetwarzanie logów...")

    ftp = FTP()
    ftp.connect(FTP_IP, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)

    # Pobierz wszystkie logi gameplay_*.log
    log_files = []
    ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
    log_files = sorted([f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")])

    if not log_files:
        print("[ERROR] Brak plików gameplay_*.log na FTP.")
        ftp.quit()
        return

    new_events = []

    for log_file in log_files:
        print(f"[INFO] Przetwarzanie logu: {log_file}")
        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {log_file}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

        for line in log_text.splitlines():
            if line not in processed_lines:
                processed_lines.add(line)
                new_events.append(line)

    ftp.quit()

    if not new_events:
        print("[INFO] Brak nowych zdarzeń w logach.")
        return

    data = {}

    # --- Parsowanie nowych zdarzeń ---
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

    # --- Jeśli plik CSV już istnieje, załaduj z niego dane i dodaj nowe ---
    if os.path.isfile(CSV_FILE):
        with open(CSV_FILE, "r", encoding="utf-8", newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row["Nick"], row["Rodzaj zamka"])
                if key not in data:
                    data[key] = {
                        "all_attempts": int(row["Wszystkie podjęte próby"]),
                        "successful_attempts": int(row["Udane"]),
                        "failed_attempts": int(row["Nieudane"]),
                        "times": []
                    }
                # Nie mamy średnich czasów per próba w CSV, więc times nie uzupełniamy

    # --- ZAPIS DO CSV (nadpisanie) ---
    with open(CSV_FILE, "w", encoding="utf-8", newline='') as f:
        fieldnames = ["Nick", "Rodzaj zamka", "Wszystkie podjęte próby", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for (nick, lock_type), stats in sorted(data.items(), key=lambda x: (x[0][0].lower(), x[0][1].lower())):
            all_attempts = stats["all_attempts"]
            succ = stats["successful_attempts"]
            fail = stats["failed_attempts"]
            avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
            eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
            writer.writerow({
                "Nick": nick,
                "Rodzaj zamka": lock_type,
                "Wszystkie podjęte próby": all_attempts,
                "Udane": succ,
                "Nieudane": fail,
                "Skuteczność": f"{eff}%",
                "Średni czas": f"{avg}s"
            })

    # --- ZAPIS PRZETWORZONYCH LINII ---
    with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
        json.dump(list(processed_lines), f, ensure_ascii=False)

    print(f"[DEBUG] Przetworzono {len(new_events)} nowych wpisów.")

    # --- GENERUJ I WYSYŁAJ TABELĘ ---
    admin_table, stats_table, podium_table = generate_tables(data)

    # Podziel wysyłkę na 3 osobne wiadomości do tego samego webhooka
    send_discord(admin_table, WEBHOOK_URL)
    send_discord(stats_table, WEBHOOK_URL)
    send_discord(podium_table, WEBHOOK_URL)

    print("[INFO] Wysłano tabele na webhook.")

# --- PĘTLA GŁÓWNA ---

def main_loop():
    # Przy starcie ściągnij wszystkie logi i utwórz plik
    process_logs()

    while True:
        time.sleep(60)
        process_logs()

# --- START ---

if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
