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

# --- IMPORTY ---
import re
import csv
import statistics
import requests
import base64
import os
from collections import defaultdict
from ftplib import FTP
from io import BytesIO

# --- FUNKCJA WYSYŁANIA NA DISCORD ---
def send_discord(content, webhook_url):
    print("[DEBUG] Wysyłanie na webhook...")
    requests.post(webhook_url, json={"content": content})

# --- FUNKCJA AKTUALIZACJI PLIKU W GITHUB ---
def update_github_file(repo, path, message, new_content, branch="main"):
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("[ERROR] Brak GITHUB_TOKEN w zmiennych środowiskowych")
        return

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    url = f"https://api.github.com/repos/{repo}/contents/{path}"

    # Pobierz SHA aktualnego pliku (wymagane do zapisu)
    r = requests.get(url, headers=headers, params={"ref": branch})
    if r.status_code == 200:
        sha = r.json()["sha"]
        existing_content = base64.b64decode(r.json()["content"]).decode("utf-8")
        new_content = existing_content + "\n" + new_content  # dopisz nowe dane
    elif r.status_code == 404:
        sha = None  # plik nie istnieje
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
WEBHOOK_TABLE1 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
WEBHOOK_TABLE2 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
WEBHOOK_TABLE3 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

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
def process_all_logs():
    print("[DEBUG] Rozpoczynam pobieranie wszystkich logów...")

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
        return

    data = {}
    user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})

    for log_name in log_files:
        print(f"[INFO] Przetwarzanie logu: {log_name}")
        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {log_name}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")
        for match in pattern.finditer(log_text):
            nick = match.group("nick")
            lock_type = match.group("lock_type")
            success = match.group("success")
            elapsed = float(match.group("elapsed"))

            user_summary[nick]["total"] += 1
            user_summary[nick]["times"].append(elapsed)
            if success == "Yes":
                user_summary[nick]["success"] += 1

            key = (nick, lock_type)
            if key not in data:
                data[key] = {"all_attempts": 0, "successful_attempts": 0, "failed_attempts": 0, "times": []}

            data[key]["all_attempts"] += 1
            if success == "Yes":
                data[key]["successful_attempts"] += 1
            else:
                data[key]["failed_attempts"] += 1
            data[key]["times"].append(elapsed)

    ftp.quit()

    # --- GENEROWANIE CSV ---
    csv_rows = []
    sorted_data = sorted(data.items(), key=lambda x: (x[0][0], lock_order.get(x[0][1], 99)))
    for (nick, lock_type), stats in sorted_data:
        all_attempts = stats["all_attempts"]
        succ = stats["successful_attempts"]
        fail = stats["failed_attempts"]
        avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
        eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
        csv_rows.append([nick, lock_type, all_attempts, succ, fail, f"{eff}%", f"{avg}s"])

    csv_content = "Nick,Rodzaj zamka,Ilość wszystkich prób,Ilość udanych prób,Ilość nieudanych prób,Skuteczność,Śr. czas\n"
    for row in csv_rows:
        csv_content += ",".join(map(str, row)) + "\n"

    # --- ZAPIS DO GITHUB ---
    update_github_file(
        repo="Spamownia/LockPick",  # <<<<<< ZMIEŃ NA SWOJE
        path="stats/logi.csv",
        message="Aktualizacja logi.csv przez Render",
        new_content=csv_content
    )

    # --- WYSYŁKA TABEL NA DISCORD ---
    # Tutaj możesz wstawić swoje funkcje generujące i wysyłające table_block, admin_block, podium_block
    # send_discord(table_block, WEBHOOK_TABLE1)
    # send_discord(admin_block, WEBHOOK_TABLE2)
    # send_discord(podium_block, WEBHOOK_TABLE3)

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    process_all_logs()
