import ftplib
import time
import os
import io
import re
import codecs
import requests

# --- Konfiguracja FTP ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"
LOG_PATTERN = "gameplay_"

# --- Webhook Discord ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Plik śledzący pobrane logi ---
STATE_FILE = "downloaded_logs.txt"

# --- Regex do lockpickingu ---
LOCKPICK_REGEX = re.compile(
    r"User:\s+([^\s]+).*?Success:\s+(Yes|No).*?Elapsed time:\s+([\d.]+).*?Lock type:\s+([^\s.]+)",
    re.IGNORECASE | re.DOTALL
)

def load_downloaded():
    if not os.path.exists(STATE_FILE):
        return set()
    with open(STATE_FILE, "r") as f:
        return set(f.read().splitlines())

def save_downloaded(files):
    with open(STATE_FILE, "w") as f:
        f.write("\n".join(files))

def fetch_logs():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    files = ftp.nlst()
    downloaded = load_downloaded()
    new_files = [f for f in files if f.startswith(LOG_PATTERN) and f not in downloaded]

    all_data = []

    for filename in new_files:
        print(f"[INFO] Downloading: {filename}")
        bio = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        bio.seek(0)
        content = codecs.decode(bio.read(), "utf-16-le")
        all_data.append(content)
        downloaded.add(filename)

    ftp.quit()
    save_downloaded(downloaded)
    return all_data

def parse_lockpicks(log_texts):
    stats = {}

    for text in log_texts:
        matches = LOCKPICK_REGEX.findall(text)
        for nick, success, elapsed, locktype in matches:
            key = (nick, locktype)
            if key not in stats:
                stats[key] = {"total": 0, "success": 0, "fail": 0, "times": []}

            stats[key]["total"] += 1
            if success.lower() == "yes":
                stats[key]["success"] += 1
            else:
                stats[key]["fail"] += 1
            stats[key]["times"].append(float(elapsed))

    return stats

def format_table(stats):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    rows = []
    for (nick, lock), data in stats.items():
        total = data["total"]
        success = data["success"]
        fail = data["fail"]
        accuracy = f"{(success / total) * 100:.2f}%"
        avg_time = f"{sum(data['times']) / len(data['times']):.2f}s"
        rows.append([nick, lock, str(total), str(success), str(fail), accuracy, avg_time])

    # Wyrównanie szerokości kolumn
    cols = list(zip(*([headers] + rows)))
    col_widths = [max(len(cell) for cell in col) for col in cols]

    def fmt_row(row):
        return " | ".join(cell.center(w) for cell, w in zip(row, col_widths))

    lines = [fmt_row(headers)]
    lines.append("-+-".join("-" * w for w in col_widths))
    for row in rows:
        lines.append(fmt_row(row))

    return "```\n" + "\n".join(lines) + "\n```"

def send_to_discord(table):
    payload = {"content": table}
    r = requests.post(WEBHOOK_URL, json=payload)
    if r.status_code == 204:
        print("[INFO] Tabela wysłana.")
    else:
        print(f"[ERROR] Błąd wysyłki: {r.status_code} {r.text}")

if __name__ == "__main__":
    while True:
        print("[INFO] Sprawdzanie logów FTP...")
        logs = fetch_logs()
        if logs:
            stats = parse_lockpicks(logs)
            if stats:
                table = format_table(stats)
                send_to_discord(table)
            else:
                print("[INFO] Brak danych lockpickingu w nowych logach.")
        else:
            print("[INFO] Brak nowych plików logów.")
        time.sleep(60)
