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

# --- FUNKCJE GITHUB ---
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
        return content
    elif r.status_code == 404:
        return ""
    else:
        print(f"[ERROR] Nie można pobrać pliku: {r.status_code} {r.text}")
        return ""

def upload_github_file(repo, path, message, new_content, branch="main"):
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
    sha = r.json()["sha"] if r.status_code == 200 else None

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

# --- FUNKCJA WYSYŁANIA NA DISCORD ---
def send_discord(content, webhook_url):
    print("[DEBUG] Wysyłanie na webhook...")
    requests.post(webhook_url, json={"content": content})

# --- KONFIGURACJA FTP ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

# --- WEBHOOKI ---
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

# --- KOLEJNOŚĆ ZAMKÓW ---
lock_order = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4}

# --- FUNKCJA GŁÓWNA ---
def process_loop():
    seen_lines = set()
    repo = "Spamownia/LockPick"

    while True:
        # Pobierz archiwalne dane z GitHub
        csv_existing = get_github_file(repo, "stats/logi.csv")
        existing_rows = []
        if csv_existing:
            reader = csv.reader(csv_existing.splitlines())
            next(reader, None)  # pomiń nagłówek
            existing_rows = [row for row in reader if len(row) == 7 and any(row)]

        # Pobierz nowe dane z FTP
        ftp = FTP()
        ftp.connect(FTP_IP, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)

        log_files = []
        ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
        log_files = sorted([f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")])
        if not log_files:
            print("[ERROR] Brak plików gameplay_*.log na FTP.")
            ftp.quit()
            time.sleep(60)
            continue

        latest_log = log_files[-1]
        print(f"[INFO] Najnowszy log: {latest_log}")

        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {latest_log}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")
        ftp.quit()

        new_lines = [line for line in log_text.splitlines() if line not in seen_lines]
        if not new_lines:
            print("[INFO] Brak nowych linii.")
            time.sleep(60)
            continue

        print(f"[INFO] Znaleziono {len(new_lines)} nowych linii.")
        seen_lines.update(new_lines)

        # Parsowanie nowych danych
        data_rows = []
        user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})
        for row in existing_rows:
            nick, lock_type, all_attempts, succ, fail, eff, avg = row
            all_attempts = int(all_attempts)
            succ = int(succ)
            fail = int(fail)
            avg_sec = float(avg.replace('s','')) if avg.endswith('s') else float(avg)
            data_rows.append([nick, lock_type, all_attempts, succ, fail, eff, avg])
            user_summary[nick]["total"] += all_attempts
            user_summary[nick]["success"] += succ
            user_summary[nick]["times"].append(avg_sec)

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
                avg = round(elapsed, 2)
                eff = round(100 * succ / all_attempts, 2)

                data_rows.append([nick, lock_type, all_attempts, succ, fail, f"{eff}%", f"{avg}s"])

        # --- Generowanie CSV ---
        csv_content = "Nick,Rodzaj zamka,Ilość wszystkich prób,Ilość udanych prób,Ilość nieudanych prób,Skuteczność,Śr. czas\n"
        for row in data_rows:
            csv_content += ",".join(map(str, row)) + "\n"

        # --- Upload CSV ---
        upload_github_file(
            repo=repo,
            path="stats/logi.csv",
            message="Aktualizacja statystyk",
            new_content=csv_content
        )

        # --- Tabela 1 (główna) ---
        table_block = "Tabela główna\n```\n"
        table_block += f"{'Nick':<10} {'Zamek':<10} {'Wszystkie':<12} {'Udane':<6} {'Nieudane':<9} {'Skut.':<8} {'Śr. czas':<8}\n"
        table_block += "-" * 70 + "\n"
        for row in data_rows:
            table_block += f"{row[0]:<10} {row[1]:<10} {str(row[2]):<12} {str(row[3]):<6} {str(row[4]):<9} {row[5]:<8} {row[6]:<8}\n"
        table_block += "```"

        # --- Tabela 2 (admin) ---
        admin_block = "Tabela admin\n```\n"
        admin_block += f"{'Nick':<10} {'Skut.':<10} {'Śr. czas':<10}\n"
        admin_block += "-" * 32 + "\n"
        for nick, summary in user_summary.items():
            total_attempts = summary["total"]
            total_success = summary["success"]
            eff = round(100 * total_success / total_attempts, 2) if total_attempts else 0
            avg = round(statistics.mean(summary["times"]), 2) if summary["times"] else 0
            admin_block += f"{nick:<10} {str(eff)+'%':<10} {str(avg)+'s':<10}\n"
        admin_block += "```"

        # --- Tabela 3 (podium) ---
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        ranking = []
        for nick, summary in user_summary.items():
            total_attempts = summary["total"]
            total_success = summary["success"]
            eff = round(100 * total_success / total_attempts, 2) if total_attempts else 0
            avg = round(statistics.mean(summary["times"]), 2) if summary["times"] else 0
            ranking.append((nick, eff, avg))
        ranking = sorted(ranking, key=lambda x: (-x[1], x[2]))[:5]

        podium_block = "Tabela podium\n```\n"
        podium_block += f"{'':<2}{'Nick':<10}{'Skut.':<10}{'Śr. czas':<10}\n"
        podium_block += "-" * 32 + "\n"
        for i, (nick, eff, avg) in enumerate(ranking):
            medal = medals[i]
            podium_block += f"{medal:<2}{nick:<10}{str(eff)+'%':<10}{str(avg)+'s':<10}\n"
        podium_block += "```"

        # --- Wysyłka wszystkich tabel na jeden webhook ---
        send_discord(table_block + "\n" + admin_block + "\n" + podium_block, WEBHOOK_URL)

        time.sleep(60)

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    threading.Thread(target=process_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
