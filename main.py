import csv
import time
import threading
import requests
from flask import Flask
from collections import defaultdict

CSV_FILE = 'logi.csv'
WEBHOOK_URL = 'https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3'  # Twój webhook

app = Flask(__name__)

def read_csv():
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = list(reader)
    return data

def generate_tables(data):
    # --- Tabela Admin ---
    admin_stats = defaultdict(lambda: {'total':0, 'success':0, 'fail':0, 'times':[]})
    for row in data:
        key = (row['Nick'], row['Rodzaj zamka'])
        admin_stats[key]['total'] += 1
        if row['Wynik'] == 'udane':
            admin_stats[key]['success'] += 1
        else:
            admin_stats[key]['fail'] += 1
        admin_stats[key]['times'].append(float(row['Czas']))

    admin_table = "**Tabela Admin**\nNick | Rodzaj zamka | Wszystkie próby | Udane | Nieudane | Skuteczność | Śr. czas\n"
    admin_table += "--- | --- | --- | --- | --- | --- | ---\n"
    for nick, lock in sorted(admin_stats.keys()):
        stats = admin_stats[(nick, lock)]
        skut = (stats['success']/stats['total'])*100 if stats['total'] else 0
        sr_czas = sum(stats['times'])/len(stats['times']) if stats['times'] else 0
        admin_table += f"{nick} | {lock} | {stats['total']} | {stats['success']} | {stats['fail']} | {skut:.1f}% | {sr_czas:.2f}s\n"

    # --- Tabela Statystyki ---
    stats_table = "**Tabela Statystyki**\nNick | Zamek | Skuteczność | Śr. czas\n"
    stats_table += "--- | --- | --- | ---\n"
    for nick, lock in sorted(admin_stats.keys()):
        stats = admin_stats[(nick, lock)]
        skut = (stats['success']/stats['total'])*100 if stats['total'] else 0
        sr_czas = sum(stats['times'])/len(stats['times']) if stats['times'] else 0
        stats_table += f"{nick} | {lock} | {skut:.1f}% | {sr_czas:.2f}s\n"

    # --- Tabela Podium ---
    podium_stats = defaultdict(lambda: {'success':0, 'total':0, 'times':[]})
    for row in data:
        nick = row['Nick']
        podium_stats[nick]['total'] += 1
        if row['Wynik'] == 'udane':
            podium_stats[nick]['success'] += 1
        podium_stats[nick]['times'].append(float(row['Czas']))

    podium_list = []
    for nick, stats in podium_stats.items():
        skut = (stats['success']/stats['total'])*100 if stats['total'] else 0
        sr_czas = sum(stats['times'])/len(stats['times']) if stats['times'] else 0
        podium_list.append((nick, skut, sr_czas))

    podium_list.sort(key=lambda x: (-x[1], x[2]))  # sortowanie: skuteczność malejąco, czas rosnąco

    podium_table = "**Tabela Podium**\n🏅 | Nick | Skuteczność | Śr. czas\n--- | --- | --- | ---\n"
    medals = ["🥇", "🥈", "🥉"]
    for i, (nick, skut, sr_czas) in enumerate(podium_list):
        medal = medals[i] if i < 3 else ""
        podium_table += f"{medal} | {nick} | {skut:.1f}% | {sr_czas:.2f}s\n"

    return admin_table, stats_table, podium_table

def process_loop():
    last_line_count = 0

    while True:
        print("[LOOP] Sprawdzanie nowych wpisów...")
        data = read_csv()
        line_count = len(data)
        print(f"[LOOP] Liczba linii w CSV: {line_count}")

        if line_count > last_line_count:
            print("[INFO] Wykryto nowe wpisy. Generuję tabele i wysyłam webhooki.")
            admin_table, stats_table, podium_table = generate_tables(data)

            # Wysyłka (na razie jeden webhook)
            requests.post(WEBHOOK_URL, json={"content": admin_table})
            requests.post(WEBHOOK_URL, json={"content": stats_table})
            requests.post(WEBHOOK_URL, json={"content": podium_table})

            last_line_count = line_count
        else:
            print("[INFO] Brak nowych wpisów.")

        time.sleep(60)  # odczekanie 60 sekund przed kolejną iteracją

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    threading.Thread(target=process_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
