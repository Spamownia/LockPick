# --- AUTOMATYCZNA INSTALACJA WYMAGANYCH BIBLIOTEK ---
import subprocess
import sys
try:
    import requests
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

import threading
import time
import csv
from flask import Flask

app = Flask(__name__)

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
CSV_FILE = 'logi.csv'

# --- FUNKCJA FORMATUJĄCA TABELĘ ---
def format_table(headers, rows):
    col_widths = [max(len(h), *(len(str(r[i])) for r in rows)) + 2 for i, h in enumerate(headers)]
    lines = []

    header_line = "".join(str(h).center(col_widths[i]) for i, h in enumerate(headers))
    separator = "".join('-'*w for w in col_widths)
    lines.append(header_line)
    lines.append(separator)

    for row in rows:
        lines.append("".join(str(row[i]).center(col_widths[i]) for i in range(len(headers))))

    return "\n".join(lines)

# --- GENEROWANIE TRZECH TABEL ---
def generate_tables(data):
    # --- ADMIN ---
    admin_headers = ["Nick", "Rodzaj zamka", "Wszystkie próby", "Udane", "Nieudane", "Skuteczność", "Śr. czas"]
    admin_rows = sorted(data, key=lambda x: (x[0], x[1]))

    admin_table = format_table(admin_headers, admin_rows)

    # --- STATYSTYKI ---
    stats_headers = ["Nick", "Zamek", "Skuteczność", "Śr. czas"]
    stats_rows = sorted([[row[0], row[1], row[5], row[6]] for row in data], key=lambda x: (x[0], x[1]))

    stats_table = format_table(stats_headers, stats_rows)

    # --- PODIUM ---
    podium_headers = ["", "Nick", "Skuteczność", "Śr. czas"]
    podium_dict = {}
    for row in data:
        nick = row[0]
        succ = float(row[5].replace("%", ""))
        time_s = float(row[6].replace("s", ""))
        if nick not in podium_dict:
            podium_dict[nick] = [0, 0, 0]  # succ_sum, time_sum, count
        podium_dict[nick][0] += succ
        podium_dict[nick][1] += time_s
        podium_dict[nick][2] += 1

    podium_rows = []
    for nick, (succ_sum, time_sum, count) in podium_dict.items():
        avg_succ = round(succ_sum / count, 2)
        avg_time = round(time_sum / count, 2)
        podium_rows.append([nick, f"{avg_succ}%", f"{avg_time}s"])

    podium_rows.sort(key=lambda x: float(x[1].replace("%", "")), reverse=True)
    medals = ["🥇", "🥈", "🥉"] + [""] * (len(podium_rows) - 3)
    podium_rows = [[medals[i]] + podium_rows[i] for i in range(len(podium_rows))]

    podium_table = format_table(podium_headers, podium_rows)

    return admin_table, stats_table, podium_table

# --- WYSYŁKA DO WEBHOOKA ---
def send_webhook(admin_table, stats_table, podium_table):
    message = f"**ADMIN**\n```{admin_table}```\n**STATYSTYKI**\n```{stats_table}```\n**PODIUM**\n```{podium_table}```"
    requests.post(WEBHOOK_URL, json={"content": message})

# --- WCZYTYWANIE CSV ---
def read_csv():
    with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # pomiń nagłówek
        data = [row for row in reader if row]
    return data

# --- MONITOROWANIE PLIKU CSV ---
def process_loop():
    prev_lines = read_csv()
    admin, stats, podium = generate_tables(prev_lines)
    send_webhook(admin, stats, podium)

    while True:
        time.sleep(60)
        current_lines = read_csv()
        if len(current_lines) == len(prev_lines):
            print("[INFO] Brak nowych wpisów.")
        else:
            new_entries = current_lines[len(prev_lines):]
            prev_lines += new_entries
            admin, stats, podium = generate_tables(prev_lines)
            send_webhook(admin, stats, podium)
            print("[INFO] Wysłano zaktualizowane tabele.")

# --- FLASK KEEPALIVE ---
@app.route('/')
def index():
    return "Lockpick monitor running."

# --- START ---
if __name__ == "__main__":
    threading.Thread(target=process_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
