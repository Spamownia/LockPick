import ftplib
import re
import csv
import json
import time
import threading
import requests
from collections import defaultdict

# --- KONFIGURACJA ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_PATTERN = re.compile(
    r"\[(?P<timestamp>[0-9:\-T\.Z]+)\]\s+LockpickEvent: Nick=(?P<Nick>[^ ]+) Zamek=(?P<Rodzaj_zamka>[^ ]+) "
    r"Attempt=(?P<Attempt>\d+) Result=(?P<Result>Success|Failure) Duration=(?P<Duration>\d+\.?\d*)"
)
PROCESSED_LINES_FILE = "/tmp/processed_lines.json"
CSV_FILE = "/tmp/logi.csv"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- FUNKCJE FTP ---
def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def list_logs(ftp):
    files = ftp.nlst()
    logs = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    logs.sort()
    return logs

def download_log(ftp, filename):
    lines = []
    def callback(line):
        lines.append(line)
    ftp.retrlines(f"RETR {filename}", callback)
    return lines

# --- PRZETWARZANIE LOGÓW ---
def parse_logs(lines):
    entries = []
    for line in lines:
        m = LOG_PATTERN.search(line)
        if m:
            entries.append({
                "timestamp": m.group("timestamp"),
                "Nick": m.group("Nick"),
                "Rodzaj zamka": m.group("Rodzaj_zamka"),
                "Attempt": int(m.group("Attempt")),
                "Result": m.group("Result"),
                "Duration": float(m.group("Duration")),
            })
    return entries

# --- PRZETWARZANIE I AGREGACJA DANYCH ---
def aggregate_data(entries):
    data = defaultdict(lambda: defaultdict(lambda: {
        "Wszystkie podjęte próby": 0,
        "Udane": 0,
        "Nieudane": 0,
        "Suma czasów": 0.0,
    }))
    for e in entries:
        nick = e["Nick"]
        zamek = e["Rodzaj zamka"]
        data[nick][zamek]["Wszystkie podjęte próby"] += 1
        if e["Result"] == "Success":
            data[nick][zamek]["Udane"] += 1
        else:
            data[nick][zamek]["Nieudane"] += 1
        data[nick][zamek]["Suma czasów"] += e["Duration"]
    return data

def calculate_stats(data):
    stats = {}
    for nick, zamki in data.items():
        stats[nick] = {}
        for zamek, vals in zamki.items():
            prob = vals["Wszystkie podjęte próby"]
            udane = vals["Udane"]
            nieudane = vals["Nieudane"]
            suma_czasow = vals["Suma czasów"]
            skutecznosc = round((udane / prob) * 100, 2) if prob > 0 else 0.0
            sredni_czas = round(suma_czasow / prob, 2) if prob > 0 else 0.0
            stats[nick][zamek] = {
                "Wszystkie podjęte próby": prob,
                "Udane": udane,
                "Nieudane": nieudane,
                "Skuteczność": skutecznosc,
                "Średni czas": sredni_czas,
            }
    return stats

# --- ZAPIS CSV ---
def save_csv(stats):
    rows = []
    for nick in sorted(stats.keys()):
        for zamek in sorted(stats[nick].keys()):
            vals = stats[nick][zamek]
            rows.append([
                nick,
                zamek,
                vals["Wszystkie podjęte próby"],
                vals["Udane"],
                vals["Nieudane"],
                vals["Skuteczność"],
                vals["Średni czas"],
            ])
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "Nick", "Rodzaj zamka", "Wszystkie podjęte próby",
            "Udane", "Nieudane", "Skuteczność", "Średni czas"
        ])
        writer.writerows(rows)

# --- GENEROWANIE TABEL ---
def generate_tables(stats):
    # Tabela Admin
    admin_lines = []
    for nick in sorted(stats.keys()):
        for zamek in sorted(stats[nick].keys()):
            v = stats[nick][zamek]
            line = f"{nick} | {zamek} | {v['Wszystkie podjęte próby']} | {v['Udane']} | {v['Nieudane']} | {v['Skuteczność']}% | {v['Średni czas']}"
            admin_lines.append(line)
    admin_table = "Tabela Admin\n" + "\n".join(admin_lines)

    # Tabela Statystyki
    stats_lines = []
    for nick in sorted(stats.keys()):
        for zamek in sorted(stats[nick].keys()):
            v = stats[nick][zamek]
            line = f"{nick} | {zamek} | {v['Skuteczność']}% | {v['Średni czas']}"
            stats_lines.append(line)
    stats_table = "Tabela Statystyki\n" + "\n".join(stats_lines)

    # Tabela Podium
    podium_data = []
    for nick in stats.keys():
        suma_skutecznosci = 0
        suma_czasu = 0
        liczba_zamkow = len(stats[nick])
        for zamek in stats[nick]:
            suma_skutecznosci += stats[nick][zamek]["Skuteczność"]
            suma_czasu += stats[nick][zamek]["Średni czas"]
        # uśredniamy średni czas i skuteczność
        if liczba_zamkow > 0:
            avg_skutecznosc = round(suma_skutecznosci, 2)  # sumujemy, nie uśredniamy - zgodnie z Twoim opisem
            avg_czas = round(suma_czasu, 2)
            podium_data.append((nick, avg_skutecznosc, avg_czas))

    podium_data.sort(key=lambda x: x[1], reverse=True)  # sort po skuteczności malejąco

    podium_lines = []
    medals = ["🥇", "🥈", "🥉"]
    for idx, (nick, skut, czas) in enumerate(podium_data):
        medal = medals[idx] if idx < 3 else ""
        line = f"{medal} | {nick} | {skut}% | {czas}"
        podium_lines.append(line)
    podium_table = "Tabela Podium\n" + "\n".join(podium_lines)

    return admin_table, stats_table, podium_table

# --- ZARZĄDZANIE PROCESSED LINES ---
def load_processed_lines():
    try:
        with open(PROCESSED_LINES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data)
    except Exception:
        return set()

def save_processed_lines(processed_set):
    with open(PROCESSED_LINES_FILE, "w", encoding="utf-8") as f:
        json.dump(list(processed_set), f, ensure_ascii=False)

# --- WYSYŁANIE WEBHOOK ---
def send_webhook(admin_table, stats_table, podium_table):
    # 3 osobne wiadomości w 3 liniach na webhook
    headers = {"Content-Type": "application/json"}
    data_admin = {"content": admin_table}
    data_stats = {"content": stats_table}
    data_podium = {"content": podium_table}

    for data in [data_admin, data_stats, data_podium]:
        r = requests.post(WEBHOOK_URL, json=data, headers=headers)
        if r.status_code != 204 and r.status_code != 200:
            print(f"[WARNING] Webhook zwrócił status {r.status_code}")

# --- LOGIKA STARTOWA ---
def initial_load_and_send():
    print("[DEBUG] Start initial_load_and_send")
    try:
        ftp = connect_ftp()
        logs = list_logs(ftp)
        print(f"[DEBUG] Lista logów do pobrania: {logs}")
        all_entries = []
        processed_lines_set = load_processed_lines()
        print(f"[DEBUG] Wczytano {len(processed_lines_set)} przetworzonych wpisów")

        for log_file in logs:
            lines = download_log(ftp, log_file)
            print(f"[DEBUG] Pobranie {len(lines)} linii z {log_file}")
            entries = parse_logs(lines)
            print(f"[DEBUG] Parsowanie logu {log_file}, znaleziono {len(entries)} pasujących wpisów")

            new_entries = []
            for e in entries:
                key = f"{e['timestamp']}|{e['Nick']}|{e['Rodzaj zamka']}|{e['Attempt']}|{e['Duration']}"
                if key not in processed_lines_set:
                    processed_lines_set.add(key)
                    new_entries.append(e)
            print(f"[DEBUG] Nowych wpisów w logu {log_file}: {len(new_entries)}")
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

        return processed_lines_set, stats
    except Exception as e:
        print(f"[ERROR] Błąd podczas początkowego ładowania i wysyłania: {e}")
        return None

def check_new_logs_loop(processed_lines_set, data):
    print("[DEBUG] Start check_new_logs_loop")
    while True:
        try:
            ftp = connect_ftp()
            logs = list_logs(ftp)
            all_entries = []
            for log_file in logs:
                lines = download_log(ftp, log_file)
                entries = parse_logs(lines)
                for e in entries:
                    key = f"{e['timestamp']}|{e['Nick']}|{e['Rodzaj zamka']}|{e['Attempt']}|{e['Duration']}"
                    if key not in processed_lines_set:
                        processed_lines_set.add(key)
                        all_entries.append(e)
            ftp.quit()

            if not all_entries:
                print("[INFO] Brak nowych wpisów w pętli.")
                time.sleep(60)
                continue

            print(f"[DEBUG] Nowych wpisów w pętli: {len(all_entries)}")

            # aktualizuj dane i statystyki
            for e in all_entries:
                nick = e["Nick"]
                zamek = e["Rodzaj zamka"]
                if nick not in data:
                    data[nick] = {}
                if zamek not in data[nick]:
                    data[nick][zamek] = {
                        "Wszystkie podjęte próby": 0,
                        "Udane": 0,
                        "Nieudane": 0,
                        "Suma czasów": 0.0,
                    }
                data[nick][zamek]["Wszystkie podjęte próby"] += 1
                if e["Result"] == "Success":
                    data[nick][zamek]["Udane"] += 1
                else:
                    data[nick][zamek]["Nieudane"] += 1
                data[nick][zamek]["Suma czasów"] += e["Duration"]

            stats = calculate_stats(data)
            save_csv(stats)
            admin_table, stats_table, podium_table = generate_tables(stats)
            send_webhook(admin_table, stats_table, podium_table)
            save_processed_lines(processed_lines_set)
            print("[INFO] Wysłano tabele po aktualizacji.")

            time.sleep(60)
        except Exception as e:
            print(f"[ERROR] Błąd w pętli sprawdzania logów: {e}")
            time.sleep(60)

# --- GŁÓWNY START ---
from flask import Flask

app = Flask(__name__)

@app.route("/")
def index():
    return "Alive"

def main_loop():
    print("[DEBUG] Start main_loop")
    res = initial_load_and_send()
    if res is None:
        print("[ERROR] Początkowe ładowanie nie powiodło się lub brak danych.")
        return
    processed_lines_set, data = res
    check_new_logs_loop(processed_lines_set, data)

if __name__ == "__main__":
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=10000, debug=False)
