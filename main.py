# --- AUTOMATYCZNA INSTALACJA (cicho) ---
import subprocess
import sys
import os
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

def silent_install(package):
    try:
        __import__(package)
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", package],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

silent_install("requests")
silent_install("flask")

# --- KONFIGURACJA ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

WEBHOOK_TABLE1 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
WEBHOOK_TABLE2 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
WEBHOOK_TABLE3 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Ścieżki zapisu - katalog /tmp jest zapisem w Render i większości chmur
BASE_DIR = "/tmp"
CSV_FILE = os.path.join(BASE_DIR, "logi.csv")
PROCESSED_FILE = os.path.join(BASE_DIR, "processed_lines.json")

# --- WZORZEC ---
pattern = re.compile(
    r"User: (?P<nick>[\w\d]+) \(\d+, [\d]+\)\. "
    r"Success: (?P<success>Yes|No)\. "
    r"Elapsed time: (?P<elapsed>[\d\.]+)\. "
    r"Failed attempts: (?P<failed_attempts>\d+)\. "
    r"Target object: [^\)]+\)\. "
    r"Lock type: (?P<lock_type>\w+)\."
)

# --- FLASK ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- FUNKCJA WYSYŁANIA NA DISCORD ---
def send_discord(content, webhook_url):
    print("[DEBUG] Wysyłanie na webhook...")
    try:
        r = requests.post(webhook_url, json={"content": content})
        if r.status_code != 204 and r.status_code != 200:
            print(f"[WARNING] Webhook zwrócił status {r.status_code}")
    except Exception as e:
        print(f"[ERROR] Błąd wysyłania webhooka: {e}")

# --- Ładowanie przetworzonych linii ---
def load_processed():
    if os.path.isfile(PROCESSED_FILE):
        try:
            with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception as e:
            print(f"[ERROR] Błąd odczytu przetworzonych linii: {e}")
    return set()

# --- Zapis przetworzonych linii ---
def save_processed(processed_lines):
    try:
        with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
            json.dump(list(processed_lines), f, ensure_ascii=False)
        print(f"[INFO] Zapisano plik JSON: {PROCESSED_FILE}")
    except Exception as e:
        print(f"[ERROR] Nie udało się zapisać pliku JSON: {e}")

# --- Funkcja do pobierania wszystkich logów z FTP i zwracania wszystkich unikalnych nowych linii ---
def fetch_all_logs():
    print("[DEBUG] Łączenie z FTP...")
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
        return []

    all_new_lines = []
    processed_lines = load_processed()

    for log_file in log_files:
        print(f"[INFO] Pobieram log: {log_file}")
        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {log_file}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")
        for line in log_text.splitlines():
            if line not in processed_lines:
                all_new_lines.append(line)
                processed_lines.add(line)

    ftp.quit()

    save_processed(processed_lines)
    print(f"[DEBUG] Łącznie nowych unikalnych linii: {len(all_new_lines)}")
    return all_new_lines

# --- Parsowanie i agregacja danych ---
def parse_and_aggregate(lines):
    data = {}
    user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})

    for entry in lines:
        match = pattern.search(entry)
        if not match:
            continue
        nick = match.group("nick")
        lock_type = match.group("lock_type")
        success = match.group("success")
        elapsed = float(match.group("elapsed"))

        # Podsumowanie per user
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

    return data, user_summary

# --- Zapis danych do CSV ---
def save_csv(data):
    os.makedirs(BASE_DIR, exist_ok=True)
    try:
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
        print(f"[INFO] Zapisano plik CSV: {CSV_FILE}")
    except Exception as e:
        print(f"[ERROR] Nie udało się zapisać pliku CSV: {e}")

# --- Generowanie tabel do Discorda ---
def generate_tables(data, user_summary):
    # Tabela Admin
    admin_lines = []
    header_admin = "Nick | Rodzaj zamka | Wszystkie podjęte próby | Udane | Nieudane | Skuteczność | Średni czas"
    admin_lines.append(header_admin)
    for (nick, lock_type), stats in sorted(data.items(), key=lambda x: (x[0][0].lower(), x[0][1].lower())):
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        fail = stats["failed_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        line = f"{nick} | {lock_type} | {all_attempts} | {succ} | {fail} | {eff}% | {avg}s"
        admin_lines.append(line)

    admin_table = "```\n" + "\n".join(admin_lines) + "\n```"

    # Tabela Statystyki
    stats_lines = []
    header_stats = "Nick | Zamek | Skuteczność | Średni czas"
    stats_lines.append(header_stats)
    for (nick, lock_type), stats in sorted(data.items(), key=lambda x: (x[0][0].lower(), x[0][1].lower())):
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        line = f"{nick} | {lock_type} | {eff}% | {avg}s"
        stats_lines.append(line)

    stats_table = "```\n" + "\n".join(stats_lines) + "\n```"

    # Tabela Podium
    medals = ["🥇", "🥈", "🥉"]
    ranking = []
    for nick, summary in user_summary.items():
        total_attempts = summary["total"]
        total_success = summary["success"]
        times_all = summary["times"]
        eff = round(100 * total_success / total_attempts, 2) if total_attempts else 0
        avg = round(statistics.mean(times_all), 2) if times_all else 0
        ranking.append((nick, eff, avg))

    ranking = sorted(ranking, key=lambda x: (-x[1], x[2]))[:5]

    podium_lines = ["", "Nick | Skuteczność | Średni czas"]
    for i, (nick, eff, avg) in enumerate(ranking):
        medal = medals[i] if i < 3 else "4️⃣" if i == 3 else "5️⃣"
        podium_lines.append(f"{medal} | {nick} | {eff}% | {avg}s")

    podium_table = "```\n" + "\n".join(podium_lines) + "\n```"

    return admin_table, stats_table, podium_table

# --- Proces aktualizacji (pobranie, przetworzenie, zapis, wysłanie) ---
def process_all():
    print("[DEBUG] Pobieram wszystkie nowe logi i parsuję...")
    new_lines = fetch_all_logs()
    if not new_lines:
        print("[INFO] Brak nowych wpisów do przetworzenia.")
        return

    data, user_summary = parse_and_aggregate(new_lines)
    save_csv(data)

    admin_table, stats_table, podium_table = generate_tables(data, user_summary)

    send_discord(admin_table, WEBHOOK_TABLE1)
    send_discord(stats_table, WEBHOOK_TABLE2)
    send_discord(podium_table, WEBHOOK_TABLE3)
    print("[INFO] Wysłano wszystkie trzy tabele.")

# --- Główna pętla co 60s, pobiera tylko nowe wpisy i aktualizuje plik ---
def main_loop():
    processed_lines = load_processed()
    while True:
        print("[DEBUG] Sprawdzam nowe logi...")
        new_lines = fetch_all_logs()
        if not new_lines:
            print("[INFO] Brak nowych wpisów w pętli.")
        else:
            print(f"[DEBUG] Nowych wpisów: {len(new_lines)}")
            data, user_summary = parse_and_aggregate(new_lines)

            # Wczytaj istniejący CSV i dołącz dane - ale uprościmy: 
            # Dla uniknięcia komplikacji i by mieć poprawne dane, załadujemy wszystkie wpisy z processed_lines i ponownie przetworzymy

            # Pobierz wszystkie linie z processed_lines, przetwórz je
            print("[DEBUG] Odtwarzam pełne dane z processed_lines...")
            # Przetwórz przetworzone linie (w tym nowe)
            all_processed_lines = list(processed_lines) + new_lines
            data_full, user_summary_full = parse_and_aggregate(all_processed_lines)

            save_csv(data_full)

            admin_table, stats_table, podium_table = generate_tables(data_full, user_summary_full)

            send_discord(admin_table, WEBHOOK_TABLE1)
            send_discord(stats_table, WEBHOOK_TABLE2)
            send_discord(podium_table, WEBHOOK_TABLE3)
            print("[INFO] Wysłano tabele po aktualizacji.")
        time.sleep(60)

# --- START ---
if __name__ == "__main__":
    # Start serwera Flask
    threading.Thread(target=main_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
