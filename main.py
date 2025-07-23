import ftplib
import re
import json
import os
import csv
import statistics
import requests
import threading
import time
from flask import Flask, request

# Konfiguracja FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"

# Katalog na lokalne tymczasowe pliki
BASE_DIR = "/tmp"
CSV_FILE = os.path.join(BASE_DIR, "logi.csv")
PROCESSED_JSON = os.path.join(BASE_DIR, "processed_lines.json")

# Webhook Discord (3 osobne wiadomości)
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def list_log_files(ftp):
    files = ftp.nlst()
    # Filtruj pliki gameplay_*.log
    log_files = [f for f in files if re.match(r"gameplay_\d{14}\.log", f)]
    return sorted(log_files)  # sortuj rosnąco

def download_log(ftp, filename):
    lines = []
    try:
        ftp.retrlines(f"RETR {filename}", callback=lines.append)
        print(f"[INFO] Pobieram log: {filename}")
    except Exception as e:
        print(f"[ERROR] Błąd pobierania {filename}: {e}")
    return lines

def parse_line(line):
    # Przykładowy format (do dopasowania do Twoich logów):
    # <Timestamp> Nick RodzajZamka Czas [SUCCESS/FAIL]
    # Przykład:
    # 2025.07.23-12.34.56: Player1 Padlock 12.5 SUCCESS

    # Dopasowanie regex:
    pattern = re.compile(r"""
        ^\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\s+       # Timestamp
        (?P<nick>\w+)\s+
        (?P<lock_type>[^\s]+)\s+
        (?P<time>[0-9]*\.?[0-9]+)\s+
        (?P<result>SUCCESS|FAIL)$
    """, re.VERBOSE)

    m = pattern.match(line)
    if not m:
        return None
    return {
        "Nick": m.group("nick"),
        "Rodzaj zamka": m.group("lock_type"),
        "Czas": float(m.group("time")),
        "Wynik": m.group("result")
    }

def load_processed_lines():
    if os.path.exists(PROCESSED_JSON):
        with open(PROCESSED_JSON, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                print(f"[INFO] Wczytano {len(data)} przetworzonych linii z JSON")
                return set(data)
            except Exception as e:
                print(f"[ERROR] Błąd wczytywania JSON: {e}")
    return set()

def save_processed_lines(lines_set):
    try:
        with open(PROCESSED_JSON, "w", encoding="utf-8") as f:
            json.dump(list(lines_set), f)
        print(f"[INFO] Zapisano plik JSON: {PROCESSED_JSON}")
    except Exception as e:
        print(f"[ERROR] Błąd zapisu JSON: {e}")

def aggregate_data(parsed_lines):
    # Struktura: data[(nick, lock_type)] = {"all_attempts": int, "successful_attempts": int, "failed_attempts": int, "times": [float]}
    data = {}
    for entry in parsed_lines:
        key = (entry["Nick"], entry["Rodzaj zamka"])
        if key not in data:
            data[key] = {
                "all_attempts": 0,
                "successful_attempts": 0,
                "failed_attempts": 0,
                "times": []
            }
        data[key]["all_attempts"] += 1
        if entry["Wynik"] == "SUCCESS":
            data[key]["successful_attempts"] += 1
        else:
            data[key]["failed_attempts"] += 1
        data[key]["times"].append(entry["Czas"])
    return data

def generate_admin_table(data):
    lines = []
    lines.append("**Admin**")
    header = "Nick | Rodzaj zamka | Wszystkie podjęte próby | Udane | Nieudane | Skuteczność | Średni czas"
    lines.append(header)
    # Sortuj po nicku, potem rodzaju zamka
    for (nick, lock), stats in sorted(data.items(), key=lambda x: (x[0][0].lower(), x[0][1].lower())):
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        fail = stats["failed_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        line = f"{nick} | {lock} | {all_attempts} | {succ} | {fail} | {eff}% | {avg}s"
        lines.append(line)
    return "\n".join(lines)

def generate_stats_table(data):
    lines = []
    lines.append("**Statystyki**")
    header = "Nick | Zamek | Skuteczność | Średni czas"
    lines.append(header)
    for (nick, lock), stats in sorted(data.items(), key=lambda x: (x[0][0].lower(), x[0][1].lower())):
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        line = f"{nick} | {lock} | {eff}% | {avg}s"
        lines.append(line)
    return "\n".join(lines)

def generate_podium_table(data):
    lines = []
    lines.append("**Podium**")
    header = "   | Nick | Skuteczność | Średni czas"
    lines.append(header)
    # Agreguj per nick: suma skuteczności * liczba zamków (dla ważenia) i średni czas średni ważony
    agg = {}
    for (nick, lock), stats in data.items():
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = (succ / all_attempts) if all_attempts else 0
        if nick not in agg:
            agg[nick] = {"eff_sum": 0, "time_sum": 0, "count": 0}
        agg[nick]["eff_sum"] += eff
        agg[nick]["time_sum"] += avg
        agg[nick]["count"] += 1

    # Oblicz średnie na nick
    podium_list = []
    for nick, vals in agg.items():
        eff_avg = round(100 * (vals["eff_sum"] / vals["count"]), 2) if vals["count"] else 0
        time_avg = round(vals["time_sum"] / vals["count"], 2) if vals["count"] else 0
        podium_list.append((nick, eff_avg, time_avg))

    # Sortuj po skuteczności malejąco
    podium_list.sort(key=lambda x: x[1], reverse=True)

    medals = ["🥇", "🥈", "🥉"]
    for i, (nick, eff, time_avg) in enumerate(podium_list):
        medal = medals[i] if i < 3 else ""
        line = f"{medal} | {nick} | {eff}% | {time_avg}s"
        lines.append(line)
    return "\n".join(lines)

def save_csv(data):
    os.makedirs(BASE_DIR, exist_ok=True)
    try:
        print(f"[DEBUG] Próba zapisu CSV do: {CSV_FILE}")
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
        print(f"[DEBUG] Zawartość katalogu {BASE_DIR}: {os.listdir(BASE_DIR)}")
    except Exception as e:
        print(f"[ERROR] Nie udało się zapisać pliku CSV: {e}")

def send_webhook(message):
    data = {"content": message}
    try:
        response = requests.post(WEBHOOK_URL, json=data)
        if response.status_code == 204:
            print("[INFO] Wysłano wiadomość na webhook.")
        else:
            print(f"[WARNING] Webhook zwrócił status {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[ERROR] Błąd wysyłania webhook: {e}")

def initial_load_and_send():
    ftp = None
    try:
        ftp = connect_ftp()
        log_files = list_log_files(ftp)
        all_lines = []
        for log_file in log_files:
            lines = download_log(ftp, log_file)
            all_lines.extend(lines)
        ftp.quit()
    except Exception as e:
        print(f"[ERROR] Błąd podczas łączenia/pobierania logów FTP: {e}")
        if ftp:
            ftp.quit()
        return None

    processed_lines_set = load_processed_lines()

    new_lines = []
    parsed_entries = []
    for line in all_lines:
        if line in processed_lines_set:
            continue
        parsed = parse_line(line)
        if parsed:
            parsed_entries.append(parsed)
            new_lines.append(line)

    if not new_lines:
        print("[INFO] Brak nowych wpisów przy początkowym ładowaniu.")
        return None

    # Dodaj nowe linie do setu
    processed_lines_set.update(new_lines)
    save_processed_lines(processed_lines_set)

    data = aggregate_data(parsed_entries)
    save_csv(data)

    admin_table = generate_admin_table(data)
    stats_table = generate_stats_table(data)
    podium_table = generate_podium_table(data)

    # Wyślij trzy osobne wiadomości na webhook
    send_webhook(admin_table)
    send_webhook(stats_table)
    send_webhook(podium_table)

    return processed_lines_set, data

def check_new_logs_loop(processed_lines_set, data):
    while True:
        try:
            print("[DEBUG] Sprawdzam nowe logi...")
            ftp = connect_ftp()
            log_files = list_log_files(ftp)
            new_lines = []
            parsed_entries = []

            for log_file in log_files:
                lines = download_log(ftp, log_file)
                for line in lines:
                    if line not in processed_lines_set:
                        parsed = parse_line(line)
                        if parsed:
                            parsed_entries.append(parsed)
                            new_lines.append(line)
            ftp.quit()

            if not new_lines:
                print("[INFO] Brak nowych wpisów w pętli.")
            else:
                print(f"[DEBUG] Nowych wpisów: {len(new_lines)}")
                processed_lines_set.update(new_lines)
                save_processed_lines(processed_lines_set)

                # Dodaj nowe dane do istniejącej agregacji
                for entry in parsed_entries:
                    key = (entry["Nick"], entry["Rodzaj zamka"])
                    if key not in data:
                        data[key] = {
                            "all_attempts": 0,
                            "successful_attempts": 0,
                            "failed_attempts": 0,
                            "times": []
                        }
                    data[key]["all_attempts"] += 1
                    if entry["Wynik"] == "SUCCESS":
                        data[key]["successful_attempts"] += 1
                    else:
                        data[key]["failed_attempts"] += 1
                    data[key]["times"].append(entry["Czas"])

                save_csv(data)

                admin_table = generate_admin_table(data)
                stats_table = generate_stats_table(data)
                podium_table = generate_podium_table(data)

                send_webhook(admin_table)
                send_webhook(stats_table)
                send_webhook(podium_table)

                print("[INFO] Wysłano tabele po aktualizacji.")

        except Exception as e:
            print(f"[ERROR] Błąd w pętli sprawdzającej logi: {e}")

        time.sleep(60)  # 60 sekund przerwy

@app.route("/", methods=["HEAD", "GET"])
def index():
    return "", 200

def main_loop():
    processed_lines_set, data = initial_load_and_send()
    if processed_lines_set is None or data is None:
        print("[ERROR] Początkowe ładowanie nie powiodło się, próbuję ponownie za 60s...")
        time.sleep(60)
        main_loop()
        return
    check_new_logs_loop(processed_lines_set, data)

if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=10000, debug=False)
