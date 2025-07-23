# --- AUTOMATYCZNA INSTALACJA (cicho) ---
import subprocess
import sys

def silent_install(package):
    try:
        __import__(package)
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", package],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

silent_install("requests")
silent_install("flask")

# --- IMPORTY ---
import re
import csv
import statistics
import requests
import base64
import os
import time
import threading
from collections import defaultdict
from ftplib import FTP
from io import BytesIO
from flask import Flask

# --- KONFIGURACJA FLASK ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- FUNKCJA WYSYŁANIA NA DISCORD ---
def send_discord(content, webhook_url):
    print("[DEBUG] Wysyłanie na webhook...")
    requests.post(webhook_url, json={"content": content})

# --- FUNKCJA POBIERANIA PLIKU Z GITHUB ---
def get_github_file(repo, path, branch="main"):
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("[ERROR] Brak GITHUB_TOKEN w zmiennych środowiskowych")
        return ""
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    r = requests.get(url, headers=headers, params={"ref": branch})
    if r.status_code == 200:
        content = base64.b64decode(r.json()["content"]).decode("utf-8")
        print(f"[INFO] Pobrano plik {path} z GitHub.")
        return content
    elif r.status_code == 404:
        print(f"[INFO] Plik {path} nie istnieje w repozytorium.")
        return ""
    else:
        print(f"[ERROR] Nie można pobrać pliku: {r.status_code} {r.text}")
        return ""

# --- FUNKCJA APPEND DO GITHUB ---
def append_github_file(repo, path, message, append_content, branch="main"):
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("[ERROR] Brak GITHUB_TOKEN w zmiennych środowiskowych")
        return
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/repos/{repo}/contents/{path}"

    r = requests.get(url, headers=headers, params={"ref": branch})
    if r.status_code == 200:
        sha = r.json()["sha"]
        existing_content = base64.b64decode(r.json()["content"]).decode("utf-8")
        new_content = existing_content + "\n" + append_content
    elif r.status_code == 404:
        sha = None
        new_content = append_content
    else:
        print(f"[ERROR] Nie można pobrać pliku: {r.status_code} {r.text}")
        return

    encoded_content = base64.b64encode(new_content.encode("utf-8")).decode("utf-8")

    data = {
        "message": message,
        "content": encoded_content,
        "branch": branch,
    }
    if sha:
        data["sha"] = sha

    r = requests.put(url, headers=headers, json=data)
    if r.status_code in [200, 201]:
        print(f"[INFO] Zaktualizowano plik {path} w repozytorium {repo}.")
    else:
        print(f"[ERROR] Nie można zaktualizować pliku: {r.status_code} {r.text}")

# --- KONFIGURACJA FTP ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

# --- WEBHOOK (WSZYSTKIE TABELKI) ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- WZORZEC ---
pattern = re.compile(
    r"User: (?P<nick>[\w\d]+) \(\d+, [\d]+\)\. "
    r"Success: (?P<success>Yes|No)\. "
    r"Elapsed time: (?P<elapsed>[\d\.]+)\. "
    r"Failed attempts: (?P<failed_attempts>\d+)\. "
    r"Target object: [^\)]+\)\. "
    r"Lock type: (?P<lock_type>\w+)\."
)

lock_order = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4}

# --- FUNKCJA GŁÓWNA ---
def process_loop():
    seen_lines = set()
    repo = "Spamownia/LockPick"  # <<<< ZMIEŃ NA SWOJE

    while True:
        ftp = FTP()
        ftp.connect(FTP_IP, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)

        log_files = []
        ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
        log_files = sorted([f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")])

        if log_files:
            latest_log = log_files[-1]
            with BytesIO() as bio:
                ftp.retrbinary(f"RETR {latest_log}", bio.write)
                log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

            new_lines = [line for line in log_text.splitlines() if line not in seen_lines]

            if new_lines:
                print(f"[INFO] Znaleziono {len(new_lines)} nowych linii.")

                # Pobierz plik CSV z GitHub
                csv_existing = get_github_file(repo, "stats/logi.csv")
                existing_rows = []
                if csv_existing:
                    reader = csv.reader(csv_existing.splitlines())
                    next(reader, None)  # pomiń nagłówek
                    existing_rows = list(reader)

                data_rows = []
                user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})

                for row in existing_rows:
                    nick, lock_type, all_attempts, succ, fail, eff, avg = row
                    user_summary[nick]["total"] += int(all_attempts)
                    user_summary[nick]["success"] += int(succ)
                    user_summary[nick]["times"].append(float(avg[:-1]))

                for line in new_lines:
                    match = pattern.search(line)
                    if match:
                        nick = match.group("nick")
                        lock_type = match.group("lock_type")
                        success = match.group("success")
                        elapsed = float(match.group("elapsed"))

                        user_summary[nick]["total"] += 1
                        user_summary[nick]["times"].append(elapsed)
                        if success == "Yes":
                            user_summary[nick]["success"] += 1

                        all_attempts = 1
                        succ = 1 if success == "Yes" else 0
                        fail = 1 - succ
                        avg = round(elapsed,2)
                        eff = round(100 * succ / all_attempts, 2)

                        data_rows.append([nick, lock_type, all_attempts, succ, fail, f"{eff}%", f"{avg}s"])

                # --- Append do GitHub ---
                csv_append = ""
                for row in data_rows:
                    csv_append += ",".join(map(str, row)) + "\n"
                append_github_file(repo, "stats/logi.csv", "Append nowych logów", csv_append)

                # --- Generowanie tabelek ---
                # Tabela główna
                table_block = "```\nNick Zamek Wszystkie Udane Nieudane Skut. Śr. czas\n"
                table_block += "-" * 70 + "\n"
                for row in data_rows:
                    table_block += " ".join(map(str, row)) + "\n"
                table_block += "```"

                # Tabela admin
                admin_block = "```\nNick Skut. Śr. czas\n"
                admin_block += "-" * 45 + "\n"
                for nick in user_summary:
                    total = user_summary[nick]["total"]
                    succ = user_summary[nick]["success"]
                    eff = round(100 * succ / total,2) if total else 0
                    avg = round(statistics.mean(user_summary[nick]["times"]),2) if user_summary[nick]["times"] else 0
                    admin_block += f"{nick} {eff}% {avg}s\n"
                admin_block += "```"

                # Tabela podium
                medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
                ranking = sorted(user_summary.items(), key=lambda x: (-round(100 * x[1]["success"] / x[1]["total"],2), statistics.mean(x[1]["times"]) if x[1]["times"] else 9999))[:5]
                podium_block = "```\n"
                for i, (nick, summary) in enumerate(ranking):
                    eff = round(100 * summary["success"] / summary["total"],2) if summary["total"] else 0
                    avg = round(statistics.mean(summary["times"]),2) if summary["times"] else 0
                    podium_block += f"{medals[i]} {nick} {eff}% {avg}s\n"
                podium_block += "```"

                # --- Wysyłka wszystkich tabelek ---
                full_message = f"**Tabela główna**\n{table_block}\n\n**Tabela admin**\n{admin_block}\n\n**Podium**\n{podium_block}"
                send_discord(full_message, WEBHOOK_URL)

                seen_lines.update(new_lines)

            else:
                print("[INFO] Brak nowych linii.")

        else:
            print("[ERROR] Brak plików logów na FTP.")

        time.sleep(60)

# --- START ---
if __name__ == "__main__":
    threading.Thread(target=process_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
