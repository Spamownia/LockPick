import os
import time
import ftplib
import re
import threading
from io import BytesIO
from datetime import datetime
from collections import defaultdict
from flask import Flask
import requests

# === Dane FTP ===
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# === Discord Webhooki ===
DISCORD_WEBHOOK_FULL = 'https://discord.com/api/webhooks/1396227086527762632/HDWBcc5rVBDbimFdh-fuE43iL8inA6YXpLuYG2a4cUmbF8RQyLqtohx-1pWaQMzBzXlf'
DISCORD_WEBHOOK_SHORT = 'https://discord.com/api/webhooks/1403070347280126132/hcMfNpXKmnnHhdylhvqvqVMnRkqzdztLf0lSQ_Lo9gs2joaqUaU0KQGBmSN8Qp88ZYaH'
DISCORD_WEBHOOK_PODIUM = 'https://discord.com/api/webhooks/1396229119456448573/PG0jkv4VBlihDwkibrn3jGZ0k516O47iTWb1dziuvoGVKVoqffLqm8GmPLbVHvpJtYhv'

# === Kolejność zamków ===
LOCK_ORDER = ['VeryEasy', 'Basic', 'Medium', 'Advanced', 'DialLock']

# === Statystyki globalne ===
stats = defaultdict(lambda: defaultdict(lambda: {
    'all': 0,
    'success': 0,
    'fail': 0,
    'total_time': 0.0
}))
known_lines = set()
last_log = None

# === Parsowanie pojedynczej linii loga ===
def parse_log_line(line):
    match = re.search(r'User: (.+?) \([0-9, ]+\).*?Success: (Yes|No).*?Elapsed time: ([\d.]+).*?Failed attempts: (\d+).*?Lock type: (\w+)', line)
    if match:
        user = match.group(1).strip()
        success = match.group(2) == "Yes"
        elapsed = float(match.group(3))
        failed_attempts = int(match.group(4))
        lock_type = match.group(5)
        return user, lock_type, success, elapsed, failed_attempts
    return None

# === Przetwarzanie linii loga ===
def process_line(line):
    parsed = parse_log_line(line)
    if not parsed:
        return

    user, lock, success, elapsed, fail_count = parsed
    stat = stats[user][lock]

    if success:
        stat['all'] += 1 + fail_count
        stat['success'] += 1
        stat['fail'] += fail_count
        stat['total_time'] += elapsed
    else:
        stat['all'] += fail_count
        stat['fail'] += fail_count

# === Pobieranie i filtrowanie logów z FTP ===
def fetch_logs():
    global last_log
    logs = []

    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)
        files = ftp.nlst()
        log_files = sorted([f for f in files if f.startswith('gameplay_') and f.endswith('.log')])

        for filename in log_files:
            bio = BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode('utf-16le', errors='ignore')
            logs.append((filename, content))

        if log_files:
            last_log = log_files[-1]

    return logs

# === Generowanie tabeli pełnej ===
def generate_full_table():
    headers = ['Nick', 'Zamek', 'Wszystkie', 'Udane', 'Nieudane', 'Skuteczność', 'Średni czas']
    col_widths = [max(len(h), 10) for h in headers]
    rows = []

    for user in sorted(stats.keys()):
        for lock in LOCK_ORDER:
            data = stats[user].get(lock)
            if not data or data['all'] == 0:
                continue
            skutecznosc = f"{(data['success'] / data['all']) * 100:.2f}%"
            avg_time = f"{(data['total_time'] / data['success']):.2f}s" if data['success'] else "-"
            row = [user, lock, str(data['all']), str(data['success']), str(data['fail']), skutecznosc, avg_time]
            rows.append(row)
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(cell))

    header = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
    separator = "|-" + "-|-".join("-" * col_widths[i] for i in range(len(headers))) + "-|"
    table_rows = ["| " + " | ".join(row[i].ljust(col_widths[i]) for i in range(len(row))) + " |" for row in rows]
    return "\n".join([header, separator] + table_rows) if rows else "| Brak danych |"

# === Generowanie tabeli skróconej ===
def generate_short_table():
    headers = ['Nick', 'Zamek', 'Skuteczność', 'Średni czas']
    col_widths = [max(len(h), 10) for h in headers]
    rows = []

    for user in sorted(stats.keys()):
        for lock in LOCK_ORDER:
            data = stats[user].get(lock)
            if not data or data['all'] == 0:
                continue
            skutecznosc = f"{(data['success'] / data['all']) * 100:.2f}%"
            avg_time = f"{(data['total_time'] / data['success']):.2f}s" if data['success'] else "-"
            row = [user, lock, skutecznosc, avg_time]
            rows.append(row)
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(cell))

    header = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
    separator = "|-" + "-|-".join("-" * col_widths[i] for i in range(len(headers))) + "-|"
    table_rows = ["| " + " | ".join(row[i].ljust(col_widths[i]) for i in range(len(row))) + " |" for row in rows]
    return "\n".join([header, separator] + table_rows) if rows else "| Brak danych |"

# === Generowanie podium ===
def generate_podium_table():
    ranking = []
    for user in stats:
        total_success = sum(stats[user][lock]['success'] for lock in LOCK_ORDER)
        total_all = sum(stats[user][lock]['all'] for lock in LOCK_ORDER)
        if total_all == 0:
            continue
        skutecznosc = (total_success / total_all) * 100
        ranking.append((user, skutecznosc))

    ranking.sort(key=lambda x: x[1], reverse=True)

    headers = ['🏅', 'Nick', 'Skuteczność']
    col_widths = [2, 10, 12]
    rows = []

    for idx, (user, skutecznosc) in enumerate(ranking):
        emoji = "🥇" if idx == 0 else "🥈" if idx == 1 else "🥉" if idx == 2 else str(idx + 1)
        row = [emoji, user, f"{skutecznosc:.2f}%"]
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))
        rows.append(row)

    header = "| " + " | ".join(headers[i].ljust(col_widths[i]) for i in range(3)) + " |"
    separator = "|-" + "-|-".join("-" * col_widths[i] for i in range(3)) + "-|"
    table_rows = ["| " + " | ".join(row[i].ljust(col_widths[i]) for i in range(3)) + " |" for row in rows]
    return "\n".join([header, separator] + table_rows) if rows else "| Brak danych |"

# === Wysyłanie danych do Discorda ===
def send_to_discord(table_full, table_short, table_podium):
    webhooks = [
        (DISCORD_WEBHOOK_FULL, table_full),
        (DISCORD_WEBHOOK_SHORT, table_short),
        (DISCORD_WEBHOOK_PODIUM, table_podium),
    ]
    for url, content in webhooks:
        data = {
            "content": "```\n" + content + "\n```"
        }
        try:
            r = requests.post(url, json=data, timeout=10)
            if r.status_code != 204:
                print(f"[ERROR] Discord HTTP {r.status_code}: {r.text}")
        except Exception as e:
            print(f"[ERROR] Discord post failed: {e}")

# === Przetwarzanie logów przy starcie ===
def process_all_logs():
    print("🔁 Uruchamianie pełnego przetwarzania logów...")
    logs = fetch_logs()
    for _, content in logs:
        for line in content.splitlines():
            if line not in known_lines:
                process_line(line)
                known_lines.add(line)
    print("[INFO] Przetworzono wszystkie dostępne logi.")

    # Generuj i wyślij wszystkie tabele
    table_full = generate_full_table()
    table_short = generate_short_table()
    table_podium = generate_podium_table()
    print(table_full)
    print(table_short)
    print(table_podium)
    send_to_discord(table_full, table_short, table_podium)

# === Monitoring nowych wpisów ===
def background_worker():
    global last_log
    print("🔁 Start wątku do monitorowania nowych linii w najnowszym pliku...")
    while True:
        try:
            logs = fetch_logs()
            current_log = None
            for fname, content in logs:
                if fname == last_log:
                    current_log = content
                    break
            if current_log:
                new_lines = []
                for line in current_log.splitlines():
                    if line not in known_lines:
                        known_lines.add(line)
                        process_line(line)
                        new_lines.append(line)

                if new_lines:
                    print(f"[INFO] Wykryto {len(new_lines)} nowych wpisów.")
                    table_full = generate_full_table()
                    table_short = generate_short_table()
                    table_podium = generate_podium_table()
                    print(table_full)
                    print(table_short)
                    print(table_podium)
                    send_to_discord(table_full, table_short, table_podium)
        except Exception as e:
            print(f"[ERROR] Błąd w tle: {e}")

        time.sleep(60)

# === Flask endpoint ===
app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    return "Lockpicking stat collector is running."

# === Start aplikacji ===
if __name__ == "__main__":
    process_all_logs()
    threading.Thread(target=background_worker, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
