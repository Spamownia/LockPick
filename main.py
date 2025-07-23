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

# --- FLASK APP ---
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- FUNKCJA WYSYŁANIA NA DISCORD ---
def send_discord(content, webhook_url):
    print("[DEBUG] Wysyłanie na webhook...")
    requests.post(webhook_url, json={"content": content})

# --- FUNKCJA AKTUALIZACJI PLIKU W GITHUB ---
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

    # Pobierz SHA i aktualną zawartość
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

# --- WEBHOOKI ---
WEBHOOK_TABLE1 = WEBHOOK_TABLE2 = WEBHOOK_TABLE3 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

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

# --- FUNKCJA PĘTLI ---
def process_loop():
    seen_lines = []
    user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})

    # Pobierz istniejące dane z GitHub
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        url = "https://api.github.com/repos/Spamownia/LockPick/contents/stats/logi.csv"
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            csv_existing = base64.b64decode(r.json()["content"]).decode("utf-8")
            reader = csv.reader(csv_existing.splitlines())
            for row in reader:
                if row and row[0] != "Nick" and len(row) == 7:
                    nick, lock_type, all_attempts, succ, fail, eff, avg = row
                    all_attempts = int(all_attempts)
                    succ = int(succ)
                    fail = int(fail)
                    avg_sec = float(avg.replace('s','')) if avg.endswith('s') else float(avg)
                    user_summary[nick]["total"] += all_attempts
                    user_summary[nick]["success"] += succ
                    user_summary[nick]["times"].append(avg_sec)
        else:
            print("[INFO] Brak istniejących danych w repozytorium.")

    while True:
        print("[DEBUG] Sprawdzam nowe linie logu...")

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
        seen_lines += new_lines

        data_rows = []
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

                all_attempts = user_summary[nick]["total"]
                succ = user_summary[nick]["success"]
                fail = all_attempts - succ
                avg = round(statistics.mean(user_summary[nick]["times"]),2)
                eff = round(100 * succ / all_attempts, 2)

                data_rows.append([nick, lock_type, all_attempts, succ, fail, f"{eff}%", f"{avg}s"])

        # --- Generowanie CSV append ---
        csv_append = ""
        for row in data_rows:
            csv_append += ",".join(map(str, row)) + "\n"

        append_github_file(
            repo="Spamownia/LockPick",
            path="stats/logi.csv",
            message="Append nowych logów",
            append_content=csv_append
        )

        # --- Wysyłka trzech tabel ---
        table_block = "Tabela Główna\n" + csv_append
        send_discord(table_block, WEBHOOK_TABLE1)

        admin_block = "Tabela Admin\n" + csv_append
        send_discord(admin_block, WEBHOOK_TABLE2)

        podium_block = "Tabela Podium\n" + csv_append
        send_discord(podium_block, WEBHOOK_TABLE3)

        time.sleep(60)

# --- URUCHOMIENIE WĄTKU I FLASK ---
if __name__ == "__main__":
    t = threading.Thread(target=process_loop)
    t.start()
    app.run(host="0.0.0.0", port=10000)
