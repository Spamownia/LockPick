import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

LOGS_DIR = "logs"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
PROCESS_INTERVAL = 60  # co ile sekund sprawdzamy nowe logi

# Słownik zbiorczy:
# { nick: { "zamek": str, "all": int, "success": int, "fail": int, "sum_time": float } }
stats = {}

# Śledzenie, ile wierszy przeczytano w każdym pliku:
# { filename: int }
lines_read = {}

def parse_log_line(line):
    parts = line.strip().split(";")
    if len(parts) != 4:
        return None
    nick, zamek, status, czas = parts
    status = status.lower()
    if status not in ("success", "fail"):
        return None
    try:
        czas = float(czas)
    except ValueError:
        return None
    return nick, zamek, status, czas

def update_stats_from_line(line):
    parsed = parse_log_line(line)
    if not parsed:
        return
    nick, zamek, status, czas = parsed
    if nick not in stats:
        stats[nick] = {"zamek": zamek, "all": 0, "success": 0, "fail": 0, "sum_time": 0.0}
    user = stats[nick]

    # Aktualizujemy statystyki
    user["all"] += 1
    if status == "success":
        user["success"] += 1
    else:
        user["fail"] += 1
    user["sum_time"] += czas

def process_full_logs():
    global stats, lines_read
    stats = {}
    lines_read = {}
    if not os.path.exists(LOGS_DIR):
        print(f"[INFO] Folder {LOGS_DIR} nie istnieje.")
        return
    files = sorted([f for f in os.listdir(LOGS_DIR) if f.endswith(".log")])
    for fname in files:
        fpath = os.path.join(LOGS_DIR, fname)
        with open(fpath, encoding="utf-8") as f:
            count = 0
            for line in f:
                update_stats_from_line(line)
                count += 1
        lines_read[fname] = count

def process_new_lines_in_latest_log():
    if not os.path.exists(LOGS_DIR):
        return False
    files = [f for f in os.listdir(LOGS_DIR) if f.endswith(".log")]
    if not files:
        return False
    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(LOGS_DIR, f)))
    fpath = os.path.join(LOGS_DIR, latest_file)
    already_read = lines_read.get(latest_file, 0)
    new_lines_count = 0

    with open(fpath, encoding="utf-8") as f:
        for _ in range(already_read):
            next(f, None)
        for line in f:
            update_stats_from_line(line)
            new_lines_count += 1
    if new_lines_count > 0:
        lines_read[latest_file] = already_read + new_lines_count
        return True
    return False

def generate_table_text():
    header = (
        "|   Nick   |  Zamek   | Wszystkie | Udane | Nieudane | Skuteczność | Średni_czas |\n"
        "|----------|----------|-----------|-------|----------|-------------|-------------|"
    )
    rows = []
    for nick, data in sorted(stats.items()):
        all_ = data["all"]
        success = data["success"]
        fail = data["fail"]
        skutecznosc = (success / all_ * 100) if all_ > 0 else 0
        sredni_czas = (data["sum_time"] / all_) if all_ > 0 else 0
        row = (
            f"| {nick:<8} | {data['zamek']:<8} | "
            f"{all_:^9} | {success:^5} | {fail:^8} | "
            f"{skutecznosc:>9.1f}% | {sredni_czas:>11.2f}s |"
        )
        rows.append(row)
    return header + "\n" + "\n".join(rows)

def send_to_discord(message: str):
    payload = {"content": f"```\n{message}\n```"}
    try:
        r = requests.post(WEBHOOK_URL, json=payload)
        if r.status_code != 204:
            print(f"[ERROR] Discord webhook odpowiedź: {r.status_code} {r.text}")
    except Exception as e:
        print(f"[ERROR] Wyjątek przy wysyłaniu na Discord: {e}")

def background_worker():
    while True:
        try:
            new_data = process_new_lines_in_latest_log()
            if new_data:
                table = generate_table_text()
                send_to_discord(table)
            time.sleep(PROCESS_INTERVAL)
        except Exception as e:
            print(f"[ERROR] Wyjątek w wątku: {e}")
            time.sleep(PROCESS_INTERVAL)

@app.route("/")
def index():
    return "Serwer działa. Statystyki logów zbierane i wysyłane na Discord."

if __name__ == "__main__":
    print("🔁 Uruchamianie pełnego przetwarzania logów...")
    process_full_logs()
    print("🔁 Start wątku do monitorowania nowych linii w najnowszym pliku...")
    threading.Thread(target=background_worker, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
