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

# --- PLIK CSV ---
CSV_FILE = "logi.csv"

# --- ŁADOWANIE PRZETWORZONYCH LINII ---
processed_lines_file = "processed_lines.json"
if os.path.isfile(processed_lines_file):
    with open(processed_lines_file, "r", encoding="utf-8") as f:
        processed_lines = set(json.load(f))
else:
    processed_lines = set()

# --- FUNKCJE PRZETWARZAJĄCE DANE ---

def load_csv():
    """Wczytuje dane z CSV i zwraca listę słowników."""
    if not os.path.isfile(CSV_FILE):
        return []
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

def save_csv(data):
    """Zapisuje listę słowników do CSV."""
    with open(CSV_FILE, "w", newline='', encoding="utf-8") as f:
        fieldnames = ["Nick", "Rodzaj zamka", "Wszystkie podjęte próby", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def update_data_with_new_events(existing_data, new_events):
    """Aktualizuje istniejące dane o nowe wpisy z new_events (lista linijek logów)."""
    # Konwersja istniejących danych na strukturę {(nick, zamek): {...stats}}
    stats = {}
    for row in existing_data:
        key = (row["Nick"], row["Rodzaj zamka"])
        stats[key] = {
            "all": int(row["Wszystkie podjęte próby"]),
            "success": int(row["Udane"]),
            "fail": int(row["Nieudane"]),
            "times": []
        }
        # W CSV nie mamy oryginalnych czasów, dlatego trzeba założyć średni czas * all = suma czasu i potem podzielić ponownie
        avg_time_str = row["Średni czas"].strip().rstrip("s")
        avg_time = float(avg_time_str) if avg_time_str else 0
        total_time = avg_time * stats[key]["all"]
        stats[key]["total_time"] = total_time

    # Parsowanie nowych eventów i aktualizacja stats
    for entry in new_events:
        m = pattern.search(entry)
        if not m:
            continue
        nick = m.group("nick")
        lock_type = m.group("lock_type")
        success = m.group("success")
        elapsed = float(m.group("elapsed"))
        key = (nick, lock_type)
        if key not in stats:
            stats[key] = {"all": 0, "success": 0, "fail": 0, "times": [], "total_time": 0.0}
        stats[key]["all"] += 1
        if success == "Yes":
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1
        stats[key]["total_time"] += elapsed

    # Przeliczenie średnich czasów i skuteczności
    new_data = []
    for (nick, lock_type), s in stats.items():
        all_attempts = s["all"]
        succ = s["success"]
        fail = s["fail"]
        total_time = s.get("total_time", 0.0)
        avg_time = round(total_time / all_attempts, 2) if all_attempts else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        new_data.append({
            "Nick": nick,
            "Rodzaj zamka": lock_type,
            "Wszystkie podjęte próby": all_attempts,
            "Udane": succ,
            "Nieudane": fail,
            "Skuteczność": f"{eff}%",
            "Średni czas": f"{avg_time}s"
        })

    # Sortowanie po nicku, potem po rodzaju zamka
    new_data.sort(key=lambda x: (x["Nick"].lower(), x["Rodzaj zamka"].lower()))

    return new_data

def generate_tables(data):
    """Generuje trzy tabele (Admin, Statystyki, Podium) na podstawie danych w formacie listy słowników."""
    # --- Tabela Admin ---
    admin_lines = []
    admin_header = ["Nick", "Rodzaj zamka", "Wszystkie podjęte próby", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    admin_lines.append(" | ".join(admin_header))
    admin_lines.append("-" * (len(admin_lines[0]) + 10))

    for row in data:
        line = f"{row['Nick']} | {row['Rodzaj zamka']} | {row['Wszystkie podjęte próby']} | {row['Udane']} | {row['Nieudane']} | {row['Skuteczność']} | {row['Średni czas']}"
        admin_lines.append(line)
    admin_table = "```\n" + "\n".join(admin_lines) + "\n```"

    # --- Tabela Statystyki ---
    stat_lines = []
    stat_header = ["Nick", "Zamek", "Skuteczność", "Średni czas"]
    stat_lines.append(" | ".join(stat_header))
    stat_lines.append("-" * (len(stat_lines[0]) + 10))

    for row in data:
        line = f"{row['Nick']} | {row['Rodzaj zamka']} | {row['Skuteczność']} | {row['Średni czas']}"
        stat_lines.append(line)
    stats_table = "```\n" + "\n".join(stat_lines) + "\n```"

    # --- Tabela Podium ---
    # Sumowanie skuteczności i średnich czasów per nick (z wszystkich zamków)
    summary = defaultdict(lambda: {"success_sum": 0, "attempts_sum": 0, "time_sum": 0.0})
    for row in data:
        nick = row["Nick"]
        # Skuteczność bez znaku %
        eff_val = float(row["Skuteczność"].rstrip("%"))
        attempts = int(row["Wszystkie podjęte próby"])
        avg_time_str = row["Średni czas"].rstrip("s")
        avg_time = float(avg_time_str) if avg_time_str else 0
        summary[nick]["success_sum"] += eff_val * attempts / 100 * attempts  # poprawka niżej
        summary[nick]["attempts_sum"] += attempts
        summary[nick]["time_sum"] += avg_time * attempts

    # Korekta: powyższa linijka nie jest poprawna, bo eff_val jest procentem, a nie liczba udanych prób.  
    # Lepiej liczyć sumę udanych prób z danych:
    # Udane mamy w polu "Udane"
    # Zatem trzeba to poprawić:

    # Przeliczenie podsumowania poprawnie:
    summary = defaultdict(lambda: {"success": 0, "attempts": 0, "time": 0.0})
    for row in data:
        nick = row["Nick"]
        succ = int(row["Udane"])
        attempts = int(row["Wszystkie podjęte próby"])
        avg_time_str = row["Średni czas"].rstrip("s")
        avg_time = float(avg_time_str) if avg_time_str else 0
        summary[nick]["success"] += succ
        summary[nick]["attempts"] += attempts
        summary[nick]["time"] += avg_time * attempts

    podium_list = []
    for nick, s in summary.items():
        attempts = s["attempts"]
        success = s["success"]
        total_time = s["time"]
        eff = round(100 * success / attempts, 2) if attempts else 0
        avg_time = round(total_time / attempts, 2) if attempts else 0
        podium_list.append((nick, eff, avg_time))

    # Sortowanie: skuteczność malejąco, średni czas rosnąco
    podium_list.sort(key=lambda x: (-x[1], x[2]))

    medals = ["🥇", "🥈", "🥉"]
    podium_lines = []
    podium_header = ["", "Nick", "Skuteczność", "Średni czas"]
    podium_lines.append(" | ".join(podium_header))
    podium_lines.append("-" * (len(podium_lines[0]) + 10))
    for i, (nick, eff, avg) in enumerate(podium_list):
        medal = medals[i] if i < 3 else ""
        line = f"{medal} | {nick} | {eff}% | {avg}s"
        podium_lines.append(line)
    podium_table = "```\n" + "\n".join(podium_lines) + "\n```"

    return admin_table, stats_table, podium_table

# --- FUNKCJA GŁÓWNA PRZETWARZAJĄCA LOGI ---

def process_logs(send_tables=True):
    global processed_lines

    print("[DEBUG] Rozpoczynam przetwarzanie logów...")

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
    print(f"[INFO] Przetwarzanie logu: {latest_log}")

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

    # Wczytanie istniejących danych CSV
    existing_data = load_csv()

    # Aktualizacja danych o nowe eventy
    updated_data = update_data_with_new_events(existing_data, new_events)

    # Zapis zaktualizowanego CSV
    save_csv(updated_data)

    # Generowanie tabel
    if send_tables:
        admin_table, stats_table, podium_table = generate_tables(updated_data)

        # Wysyłanie tabel osobno (oddzielne wiadomości)
        send_discord(admin_table, WEBHOOK_URL)
        send_discord(stats_table, WEBHOOK_URL)
        send_discord(podium_table, WEBHOOK_URL)
        print("[INFO] Wysłano wszystkie trzy tabele.")

    # Zapisanie przetworzonych linii
    with open(processed_lines_file, "w", encoding="utf-8") as f:
        json.dump(list(processed_lines), f, ensure_ascii=False)

# --- FUNKCJA POCZĄTKOWA - WYSYŁA PEŁNE DANE Z CSV NA START ---
def initial_send():
    data = load_csv()
    if not data:
        print("[INFO] Brak danych w CSV przy starcie, nie wysyłam tabel.")
        return
    admin_table, stats_table, podium_table = generate_tables(data)
    send_discord(admin_table, WEBHOOK_URL)
    send_discord(stats_table, WEBHOOK_URL)
    send_discord(podium_table, WEBHOOK_URL)
    print("[INFO] Wysłano tabele startowe z CSV.")

# --- PĘTLA GŁÓWNA ---

def main_loop():
    # Najpierw wyślij tabele z obecnych danych
    initial_send()
    while True:
        process_logs()
        time.sleep(60)

# --- START ---

if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
