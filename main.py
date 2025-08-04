import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

LOGS_DIR = "logs"
WEBHOOK_URL = "TWÓJ_DISCORD_WEBHOOK_URL"  # <-- tu wpisz swój webhook Discord
PROCESS_INTERVAL = 60

# Dane zbiorcze: {nick: {zamek, all, success, fail, sum_time}}
stats = {}

def parse_log_line(line):
    # Zakładam format: "Nick;Zamek;Status;Czas"
    # Przykład: "Anu;Advanced;fail;12.67"
    parts = line.strip().split(";")
    if len(parts) != 4:
        return None
    nick, zamek, status, czas = parts
    try:
        czas = float(czas)
    except ValueError:
        return None
    return nick, zamek, status.lower(), czas

def update_stats_from_line(line):
    parsed = parse_log_line(line)
    if not parsed:
        return
    nick, zamek, status, czas = parsed
    if nick not in stats:
        stats[nick] = {
            "zamek": zamek,
            "all": 0,
            "success": 0,
            "fail": 0,
            "sum_time": 0.0,
        }
    user_stats = stats[nick]
    user_stats["all"] += 1
    if status == "success":
        user_stats["success"] += 1
    else:
        user_stats["fail"] += 1
    user_stats["sum_time"] += czas

def process_all_logs():
    global stats
    stats = {}  # reset przy pełnym przetworzeniu
    if not os.path.exists(LOGS_DIR):
        return
    for fname in sorted(os.listdir(LOGS_DIR)):
        fpath = os.path.join(LOGS_DIR, fname)
        if not os.path.isfile(fpath):
            continue
        with open(fpath, encoding="utf-8") as f:
            for line in f:
                update_stats_from_line(line)

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

def send_to_discord(message):
    payload = {"content": f"```\n{message}\n```"}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code != 204:
            print(f"[Discord] Błąd wysyłki: {response.status_code} {response.text}")
    except Exception as e:
        print(f"[Discord] Wyjątek podczas wysyłki: {e}")

def periodic_worker():
    last_processed_files = set()
    while True:
        try:
            current_files = set(os.listdir(LOGS_DIR)) if os.path.exists(LOGS_DIR) else set()
            new_files = current_files - last_processed_files
            if new_files:
                # Przetwórz wszystkie logi na nowo (sumowanie zbiorcze)
                process_all_logs()
                table_text = generate_table_text()
                send_to_discord(table_text)
                last_processed_files = current_files
            time.sleep(PROCESS_INTERVAL)
        except Exception as e:
            print(f"[Worker] Wyjątek w pętli: {e}")
            time.sleep(PROCESS_INTERVAL)

@app.route("/")
def home():
    return "Skrypt działa. Statystyki zbiorcze są aktualizowane i wysyłane na Discord."

if __name__ == "__main__":
    print("🔁 Uruchamianie skanowania logów...")
    process_all_logs()  # na start
    threading.Thread(target=periodic_worker, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
