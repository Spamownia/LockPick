import ftplib
import re
import json
import csv
import time
import threading
import requests
from collections import defaultdict
from datetime import datetime

# --- KONFIGURACJA ---

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"

LOG_FILE_PATTERN = r"gameplay_.*\.log"
PROCESSED_LINES_FILE = "/tmp/processed_lines.json"
CSV_FILE = "/tmp/logi.csv"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- FUNKCJE FTP ---

def connect_ftp():
    print("[DEBUG] Łączenie z FTP...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    print("[DEBUG] Połączono z FTP")
    return ftp

def list_logs(ftp):
    files = ftp.nlst()
    logs = [f for f in files if re.match(LOG_FILE_PATTERN, f)]
    return logs

def download_log(ftp, filename):
    lines = []
    def handle_line(line):
        lines.append(line)
    ftp.retrlines(f"RETR {filename}", callback=handle_line)
    return lines

# --- PRZETWARZANIE LOGÓW ---

LOG_LINE_REGEX = re.compile(
    r"^(?P<timestamp>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): "
    r"Nick: (?P<Nick>.+?) - "
    r"Zamek: (?P<Rodzaj_zamka>.+?) - "
    r"Próba: (?P<Attempt>\d+) - "
    r"Czas: (?P<Duration>\d+\.\d+)"
)

def parse_logs(lines):
    entries = []
    for line in lines:
        m = LOG_LINE_REGEX.match(line)
        if m:
            d = m.groupdict()
            entries.append({
                "timestamp": d["timestamp"],
                "Nick": d["Nick"],
                "Rodzaj zamka": d["Rodzaj_zamka"],
                "Attempt": int(d["Attempt"]),
                "Duration": float(d["Duration"])
            })
    return entries

# --- PRZETWARZANIE DANYCH ---

def load_processed_lines():
    try:
        with open(PROCESSED_LINES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"[DEBUG] Wczytano {len(data)} przetworzonych wpisów")
            return set(data)
    except FileNotFoundError:
        print("[DEBUG] Brak pliku przetworzonych wpisów, zaczynam od zera")
        return set()
    except Exception as e:
        print(f"[ERROR] Błąd podczas wczytywania przetworzonych wpisów: {e}")
        return set()

def save_processed_lines(processed_lines_set):
    with open(PROCESSED_LINES_FILE, "w", encoding="utf-8") as f:
        json.dump(list(processed_lines_set), f)
    print(f"[DEBUG] Zapisano {len(processed_lines_set)} przetworzonych wpisów")

def aggregate_data(entries):
    data = defaultdict(lambda: {
        "Wszystkie podjęte próby": 0,
        "Udane": 0,
        "Nieudane": 0,
        "Czasy": []
    })

    for e in entries:
        key = (e["Nick"], e["Rodzaj zamka"])
        data[key]["Wszystkie podjęte próby"] += 1
        if e["Attempt"] == 1:
            data[key]["Udane"] += 1
        else:
            data[key]["Nieudane"] += 1
        data[key]["Czasy"].append(e["Duration"])

    # Oblicz skuteczność i średni czas
    for key, v in data.items():
        total = v["Wszystkie podjęte próby"]
        udane = v["Udane"]
        v["Skuteczność"] = round((udane / total) * 100, 2) if total > 0 else 0.0
        v["Średni czas"] = round(sum(v["Czasy"]) / len(v["Czasy"]), 2) if v["Czasy"] else 0.0

    return data

def calculate_stats(data):
    # Zamiana na listę słowników do CSV i generowania tabel
    rows = []
    for (nick, zamek), stats in data.items():
        rows.append({
            "Nick": nick,
            "Rodzaj zamka": zamek,
            "Wszystkie podjęte próby": stats["Wszystkie podjęte próby"],
            "Udane": stats["Udane"],
            "Nieudane": stats["Nieudane"],
            "Skuteczność": stats["Skuteczność"],
            "Średni czas": stats["Średni czas"]
        })
    # Sortowanie
    rows.sort(key=lambda r: (r["Nick"].lower(), r["Rodzaj zamka"].lower()))
    return rows

def save_csv(rows):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Nick", "Rodzaj zamka", "Wszystkie podjęte próby",
            "Udane", "Nieudane", "Skuteczność", "Średni czas"
        ])
        writer.writeheader()
        writer.writerows(rows)
    print(f"[INFO] Zapisano plik CSV: {CSV_FILE}")

# --- GENEROWANIE TABEL ---

def generate_tables(stats_rows):
    # Admin: Nick - Rodzaj zamka - Wszystkie podjęte próby - Udane - Nieudane - Skuteczność - Średni czas
    admin_lines = []
    for r in stats_rows:
        line = f"{r['Nick']} | {r['Rodzaj zamka']} | {r['Wszystkie podjęte próby']} | {r['Udane']} | {r['Nieudane']} | {r['Skuteczność']}% | {r['Średni czas']}s"
        admin_lines.append(line)

    # Statystyki: Nick - Zamek - Skuteczność - Średni czas
    stats_lines = []
    for r in stats_rows:
        line = f"{r['Nick']} | {r['Rodzaj zamka']} | {r['Skuteczność']}% | {r['Średni czas']}s"
        stats_lines.append(line)

    # Podium: Nick - zsumowana skuteczność i średni czas
    podium_data = defaultdict(lambda: {"Skuteczność": 0, "Średni czas": 0, "Ilość": 0})
    for r in stats_rows:
        p = podium_data[r["Nick"]]
        p["Skuteczność"] += r["Skuteczność"]
        p["Średni czas"] += r["Średni czas"]
        p["Ilość"] += 1
    podium_lines = []
    # Oblicz średni czas jako średnia z sumowanych czasów
    podium_list = []
    for nick, v in podium_data.items():
        avg_czas = round(v["Średni czas"] / v["Ilość"], 2) if v["Ilość"] > 0 else 0.0
        podium_list.append({
            "Nick": nick,
            "Skuteczność": round(v["Skuteczność"], 2),
            "Średni czas": avg_czas
        })
    # Sortuj po skuteczności malejąco, potem po średnim czasie rosnąco
    podium_list.sort(key=lambda x: (-x["Skuteczność"], x["Średni czas"]))
    medals = ["🥇", "🥈", "🥉"]
    for i, p in enumerate(podium_list):
        medal = medals[i] if i < 3 else ""
        line = f"{medal} | {p['Nick']} | {p['Skuteczność']}% | {p['Średni czas']}s"
        podium_lines.append(line)

    return admin_lines, stats_lines, podium_lines

# --- WYSYŁANIE NA WEBHOOK ---

def send_webhook(admin_lines, stats_lines, podium_lines):
    # Każda tabela w osobnej wiadomości, każda linia w nowej linii
    for title, lines in [("Admin", admin_lines), ("Statystyki", stats_lines), ("Podium", podium_lines)]:
        content = f"**{title}**\n" + "\n".join(lines)
        data = {"content": content}
        response = requests.post(WEBHOOK_URL, json=data)
        if response.status_code != 204 and response.status_code != 200:
            print(f"[WARNING] Webhook zwrócił status {response.status_code} dla {title}")
        else:
            print(f"[INFO] Wysłano tabelę: {title}")

# --- FUNKCJE GŁÓWNE ---

def initial_load_and_send():
    print("[DEBUG] Start initial_load_and_send")
    try:
        ftp = connect_ftp()
        logs = list_logs(ftp)
        print(f"[DEBUG] Lista logów do pobrania: {logs}")
        if not logs:
            print("[WARNING] Nie znaleziono żadnych logów na FTP.")
            ftp.quit()
            return set(), []

        all_entries = []
        processed_lines_set = load_processed_lines()

        for log_file in logs:
            print(f"[INFO] Pobieram log: {log_file}")
            lines = download_log(ftp, log_file)
            print(f"[INFO] Pobrano {len(lines)} linii z {log_file}")
            entries = parse_logs(lines)
            print(f"[INFO] Parsowanie logu {log_file}, znaleziono {len(entries)} pasujących wpisów")

            new_entries = []
            for e in entries:
                key = f"{e['timestamp']}|{e['Nick']}|{e['Rodzaj zamka']}|{e['Attempt']}|{e['Duration']}"
                if key not in processed_lines_set:
                    processed_lines_set.add(key)
                    new_entries.append(e)
            print(f"[INFO] Nowych wpisów w logu {log_file}: {len(new_entries)}")
            all_entries.extend(new_entries)

        ftp.quit()
        print("[DEBUG] Zamknięto połączenie FTP")

        if not all_entries:
            print("[INFO] Brak nowych wpisów podczas początkowego ładowania.")
            return processed_lines_set, []

        data = aggregate_data(all_entries)
        stats = calculate_stats(data)
        save_csv(stats)
        admin_table, stats_table, podium_table = generate_tables(stats)
        send_webhook(admin_table, stats_table, podium_table)
        save_processed_lines(processed_lines_set)

        return processed_lines_set, stats
    except Exception as e:
        print(f"[ERROR] Błąd podczas początkowego ładowania i wysyłania: {e}")
        return set(), []

def check_for_new_entries(processed_lines_set):
    print("[DEBUG] Sprawdzam nowe logi...")
    try:
        ftp = connect_ftp()
        logs = list_logs(ftp)
        if not logs:
            print("[WARNING] Nie znaleziono logów na FTP.")
            ftp.quit()
            return processed_lines_set, []

        new_entries = []
        for log_file in logs:
            print(f"[INFO] Pobieram log: {log_file}")
            lines = download_log(ftp, log_file)
            entries = parse_logs(lines)

            for e in entries:
                key = f"{e['timestamp']}|{e['Nick']}|{e['Rodzaj zamka']}|{e['Attempt']}|{e['Duration']}"
                if key not in processed_lines_set:
                    processed_lines_set.add(key)
                    new_entries.append(e)
        ftp.quit()
        print("[DEBUG] Zamknięto połączenie FTP")
        print(f"[DEBUG] Nowych wpisów: {len(new_entries)}")
        return processed_lines_set, new_entries
    except Exception as e:
        print(f"[ERROR] Błąd podczas sprawdzania nowych wpisów: {e}")
        return processed_lines_set, []

def main_loop():
    print("[DEBUG] Start main_loop")
    processed_lines_set, data = initial_load_and_send()
    while True:
        time.sleep(60)
        processed_lines_set, new_entries = check_for_new_entries(processed_lines_set)
        if new_entries:
            all_entries = new_entries
            data_dict = defaultdict(lambda: {
                "Wszystkie podjęte próby": 0,
                "Udane": 0,
                "Nieudane": 0,
                "Czasy": []
            })
            # Załaduj istniejące dane (z pliku CSV) do słownika
            try:
                with open(CSV_FILE, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        key = (row["Nick"], row["Rodzaj zamka"])
                        data_dict[key]["Wszystkie podjęte próby"] = int(row["Wszystkie podjęte próby"])
                        data_dict[key]["Udane"] = int(row["Udane"])
                        data_dict[key]["Nieudane"] = int(row["Nieudane"])
                        # Do czasu dokładamy średni czas * próby, bo musimy odtworzyć sumę czasów
                        data_dict[key]["Czasy"].append(float(row["Średni czas"]) * int(row["Wszystkie podjęte próby"]))
            except Exception:
                # Brak pliku lub błąd - zaczynamy od zera
                pass

            # Aktualizacja danych o nowe wpisy
            for e in all_entries:
                key = (e["Nick"], e["Rodzaj zamka"])
                data_dict[key]["Wszystkie podjęte próby"] += 1
                if e["Attempt"] == 1:
                    data_dict[key]["Udane"] += 1
                else:
                    data_dict[key]["Nieudane"] += 1
                data_dict[key]["Czasy"].append(e["Duration"])

            # Ponownie wylicz skuteczność i średni czas
            updated_data = {}
            for key, v in data_dict.items():
                total = v["Wszystkie podjęte próby"]
                udane = v["Udane"]
                updated_data[key] = {
                    "Wszystkie podjęte próby": total,
                    "Udane": udane,
                    "Nieudane": v["Nieudane"],
                    "Skuteczność": round((udane / total) * 100, 2) if total > 0 else 0.0,
                    "Średni czas": round(sum(v["Czasy"]) / len(v["Czasy"]), 2) if v["Czasy"] else 0.0
                }

            stats_rows = calculate_stats(updated_data)
            save_csv(stats_rows)
            admin_table, stats_table, podium_table = generate_tables(stats_rows)
            send_webhook(admin_table, stats_table, podium_table)
            save_processed_lines(processed_lines_set)
            print("[INFO] Wysłano tabele po aktualizacji.")
        else:
            print("[INFO] Brak nowych wpisów w pętli.")

# --- START SKRYPTU ---

if __name__ == "__main__":
    from flask import Flask, request

    app = Flask(__name__)

    @app.route("/", methods=["HEAD", "GET"])
    def index():
        return "Alive", 200

    threading.Thread(target=main_loop, daemon=True).start()

    app.run(host="0.0.0.0", port=10000, debug=False)
