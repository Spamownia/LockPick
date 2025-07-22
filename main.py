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
import time
from collections import defaultdict
from ftplib import FTP
from io import BytesIO

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
def process_new_entries(seen_lines):
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
        return seen_lines

    latest_log = log_files[-1]
    print(f"[INFO] Najnowszy log: {latest_log}")

    with BytesIO() as bio:
        ftp.retrbinary(f"RETR {latest_log}", bio.write)
        log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

    ftp.quit()

    new_lines = [line for line in log_text.splitlines() if line not in seen_lines]
    if not new_lines:
        print("[INFO] Brak nowych linii.")
        return seen_lines

    print(f"[INFO] Znaleziono {len(new_lines)} nowych linii.")

    data_rows = []
    user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})

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

    # --- Generowanie CSV append ---
    csv_append = ""
    for row in data_rows:
        csv_append += ",".join(map(str, row)) + "\n"

    # --- Append do GitHub ---
    append_github_file(
        repo="Spamownia/LockPick",  # <<<< ZMIEŃ NA SWOJE
        path="stats/logi.csv",
        message="Append nowych logów",
        append_content=csv_append
    )

    # --- Wysyłka Discord (opcjonalna) ---
    # send_discord(table_block, WEBHOOK_TABLE1)

    return seen_lines + new_lines

# --- PĘTLA ---
if __name__ == "__main__":
    seen_lines = []
    while True:
        seen_lines = process_new_entries(seen_lines)
        time.sleep(60)
