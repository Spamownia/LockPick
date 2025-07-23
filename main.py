import os
import re
import csv
import json
import time
import threading
import requests
from ftplib import FTP
from collections import defaultdict

# Konfiguracja FTP
FTP_HOST = "ftp.example.com"
FTP_PORT = 21
FTP_USER = "username"
FTP_PASS = "password"

# Ścieżki i pliki tymczasowe
PROCESSED_LINES_FILE = "/tmp/processed_lines.json"
CSV_FILE = "/tmp/logi.csv"

# Webhook Discord (3 osobne linie)
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3mVy3"

# Regex do wyciągania danych z logów
LOG_LINE_REGEX = re.compile(
    r"(?P<timestamp>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): "
    r"(?P<nick>\w+) - (?P<lock_type>[\w\s]+) - "
    r"Attempt: (?P<attempt>[SU]) - Duration: (?P<duration>\d+\.?\d*)"
)

def connect_ftp():
    print("[DEBUG] Łączenie z FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def list_logs(ftp):
    files = ftp.nlst()
    logs = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    logs.sort()
    return logs

def download_log(ftp, filename):
    print(f"[INFO] Pobieram log: {filename}")
    lines = []
    ftp.retrlines(f"RETR {filename}", lines.append)
    return lines

def parse_logs(log_lines):
    entries = []
    for line in log_lines:
        match = LOG_LINE_REGEX.search(line)
        if not match:
            continue
        data = match.groupdict()
        entries.append({
            "timestamp": data["timestamp"],
            "Nick": data["nick"],
            "Rodzaj zamka": data["lock_type"],
            "Attempt": data["attempt"],  # 'S' = success, 'U' = unsuccess
            "Duration": float(data["duration"])
        })
    return entries

def load_processed_lines():
    if not os.path.isfile(PROCESSED_LINES_FILE):
        return set()
    with open(PROCESSED_LINES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return set(data)

def save_processed_lines(processed_set):
    with open(PROCESSED_LINES_FILE, "w", encoding="utf-8") as f:
        json.dump(list(processed_set), f)

def aggregate_data(entries):
    # Struktura: data[nick][lock_type] = list of attempts (success/fail, duration)
    data = defaultdict(lambda: defaultdict(list))
    for e in entries:
        key = (e["timestamp"], e["Nick"], e["Rodzaj zamka"], e["Attempt"], e["Duration"])
        data[e["Nick"]][e["Rodzaj zamka"]].append(e)
    return data

def calculate_stats(data):
    # Dla każdej pary nick/lock_type liczymy statystyki
    stats = {}
    for nick, locks in data.items():
        stats[nick] = {}
        for lock_type, attempts in locks.items():
            total = len(attempts)
            success = sum(1 for a in attempts if a["Attempt"] == "S")
            fail = total - success
            success_rate = round((success / total) * 100, 2) if total > 0 else 0.0
            avg_time = round(sum(a["Duration"] for a in attempts) / total, 2) if total > 0 else 0.0
            stats[nick][lock_type] = {
                "Wszystkie podjęte próby": total,
                "Udane": success,
                "Nieudane": fail,
                "Skuteczność": success_rate,
                "Średni czas": avg_time
            }
    return stats

def generate_tables(stats):
    # Tabela Admin
    admin_lines = []
    admin_lines.append("Nick | Rodzaj zamka | Wszystkie podjęte próby | Udane | Nieudane | Skuteczność (%) | Średni czas")
    for nick in sorted(stats.keys()):
        for lock_type in sorted(stats[nick].keys()):
            row = stats[nick][lock_type]
            line = f"{nick} | {lock_type} | {row['Wszystkie podjęte próby']} | {row['Udane']} | {row['Nieudane']} | {row['Skuteczność']} | {row['Średni czas']}"
            admin_lines.append(line)

    # Tabela Statystyki
    stats_lines = []
    stats_lines.append("Nick | Zamek | Skuteczność (%) | Średni czas")
    for nick in sorted(stats.keys()):
        for lock_type in sorted(stats[nick].keys()):
            row = stats[nick][lock_type]
            line = f"{nick} | {lock_type} | {row['Skuteczność']} | {row['Średni czas']}"
            stats_lines.append(line)

    # Tabela Podium
    podium_lines = []
    podium_lines.append(" | Nick | Skuteczność (%) | Średni czas")
    podium_list = []
    for nick in stats.keys():
        total_success = 0.0
        total_time = 0.0
        total_attempts = 0
        for lock_type, row in stats[nick].items():
            attempts = row["Wszystkie podjęte próby"]
            total_attempts += attempts
            total_success += (row["Skuteczność"] / 100.0) * attempts
            total_time += row["Średni czas"] * attempts
        if total_attempts > 0:
            overall_success_rate = round((total_success / total_attempts) * 100, 2)
            overall_avg_time = round(total_time / total_attempts, 2)
        else:
            overall_success_rate = 0.0
            overall_avg_time = 0.0
        podium_list.append((nick, overall_success_rate, overall_avg_time))

    podium_list.sort(key=lambda x: (-x[1], x[2]))  # sortuj po skuteczności malejąco, potem czas rosnąco
    medals = ["🥇", "🥈", "🥉"]
    for i, (nick, success, avg_time) in enumerate(podium_list):
        medal = medals[i] if i < 3 else ""
        line = f"{medal} | {nick} | {success} | {avg_time}"
        podium_lines.append(line)

    return "\n".join(admin_lines), "\n".join(stats_lines), "\n".join(podium_lines)

def save_csv(stats):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["Nick", "Rodzaj zamka", "Wszystkie podjęte próby", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for nick in sorted(stats.keys()):
            for lock_type in sorted(stats[nick].keys()):
                row = stats[nick][lock_type]
                writer.writerow({
                    "Nick": nick,
                    "Rodzaj zamka": lock_type,
                    "Wszystkie podjęte próby": row["Wszystkie podjęte próby"],
                    "Udane": row["Udane"],
                    "Nieudane": row["Nieudane"],
                    "Skuteczność": row["Skuteczność"],
                    "Średni czas": row["Średni czas"]
                })

def send_webhook(admin_table, stats_table, podium_table):
    headers = {"Content-Type": "application/json"}
    data_admin = {"content": f"**Tabela Admin:**\n```\n{admin_table}\n```"}
    data_stats = {"content": f"**Tabela Statystyki:**\n```\n{stats_table}\n```"}
    data_podium = {"content": f"**Tabela Podium:**\n```\n{podium_table}\n```"}

    # Wysyłanie trzech osobnych wiadomości do webhooka
    for data in (data_admin, data_stats, data_podium):
        try:
            response = requests.post(WEBHOOK_URL, json=data, headers=headers)
            if response.status_code != 204 and response.status_code != 200:
                print(f"[WARNING] Webhook zwrócił status {response.status_code}")
        except Exception as e:
            print(f"[ERROR] Błąd przy wysyłaniu webhooka: {e}")

def initial_load_and_send():
    try:
        ftp = connect_ftp()
        logs = list_logs(ftp)
        all_entries = []
        processed_lines_set = set()

        # Wczytaj już przetworzone linie z pliku JSON
        processed_lines_set = load_processed_lines()

        for log_file in logs:
            lines = download_log(ftp, log_file)
            entries = parse_logs(lines)

            # Przechowuj tylko nowe linie, których nie było wcześniej
            new_entries = []
            for e in entries:
                key = f"{e['timestamp']}|{e['Nick']}|{e['Rodzaj zamka']}|{e['Attempt']}|{e['Duration']}"
                if key not in processed_lines_set:
                    processed_lines_set.add(key)
                    new_entries.append(e)
            all_entries.extend(new_entries)

        ftp.quit()

        if not all_entries:
            print("[INFO] Brak nowych wpisów podczas początkowego ładowania.")
            return None

        data = aggregate_data(all_entries)
        stats = calculate_stats(data)
        save_csv(stats)
        admin_table, stats_table, podium_table = generate_tables(stats)
        send_webhook(admin_table, stats_table, podium_table)

        save_processed_lines(processed_lines_set)
        print(f"[INFO] Zapisano plik CSV: {CSV_FILE}")
        print(f"[INFO] Zapisano plik JSON: {PROCESSED_LINES_FILE}")

        return processed_lines_set, data
    except Exception as e:
        print(f"[ERROR] Błąd podczas początkowego ładowania i wysyłania: {e}")
        return None

def check_new_logs_loop(processed_lines_set, data):
    while True:
        try:
            ftp = connect_ftp()
            logs = list_logs(ftp)
            new_lines_count = 0
            all_new_entries = []

            for log_file in logs:
                lines = download_log(ftp, log_file)
                entries = parse_logs(lines)
                new_entries = []
                for e in entries:
                    key = f"{e['timestamp']}|{e['Nick']}|{e['Rodzaj zamka']}|{e['Attempt']}|{e['Duration']}"
                    if key not in processed_lines_set:
                        processed_lines_set.add(key)
                        new_entries.append(e)
                if new_entries:
                    all_new_entries.extend(new_entries)
                    new_lines_count += len(new_entries)

            ftp.quit()

            if new_lines_count == 0:
                print("[INFO] Brak nowych wpisów w pętli.")
            else:
                print(f"[DEBUG] Przetworzono {new_lines_count} nowych wpisów.")
                # Aktualizuj dane i pliki
                # Scal nowe wpisy z istniejącymi
                for e in all_new_entries:
                    data[e["Nick"]][e["Rodzaj zamka"]].append(e)
                stats = calculate_stats(data)
                save_csv(stats)
                admin_table, stats_table, podium_table = generate_tables(stats)
                send_webhook(admin_table, stats_table, podium_table)
                save_processed_lines(processed_lines_set)
                print("[INFO] Wysłano tabele po aktualizacji.")

            time.sleep(60)

        except Exception as e:
            print(f"[ERROR] Błąd w pętli sprawdzania nowych logów: {e}")
            time.sleep(60)

def main_loop():
    while True:
        result = initial_load_and_send()
        if result is None:
            print("[ERROR] Początkowe ładowanie nie powiodło się lub brak nowych danych, próbuję ponownie za 60s...")
            time.sleep(60)
            continue
        processed_lines_set, data = result
        check_new_logs_loop(processed_lines_set, data)

if __name__ == "__main__":
    import flask
    from flask import Flask

    app = Flask(__name__)

    @app.route("/")
    def index():
        return "Alive"

    # Start pętli w osobnym wątku
    thread = threading.Thread(target=main_loop, daemon=True)
    thread.start()

    app.run(host="0.0.0.0", port=10000)
