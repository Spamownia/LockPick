# --- AUTOMATYCZNA INSTALACJA WYMAGANYCH BIBLIOTEK ---
import subprocess, sys

for pkg, module in [("requests", "requests"), ("pandas", "pandas")]:
    try:
        __import__(module)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

# --- IMPORTY ---
import os
from ftplib import FTP
import pandas as pd
import requests

# --- DANE FTP I WEBHOOK ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- POBIERANIE NAJNOWSZEGO LOGA ---
ftp = FTP()
ftp.connect(FTP_HOST, FTP_PORT)
ftp.login(FTP_USER, FTP_PASS)
ftp.cwd(LOG_DIR)
files = ftp.nlst()

# Filtruj pliki gameplay_*.log
log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

if not log_files:
    print("[ERROR] Brak plików gameplay_*.log")
    ftp.quit()
    exit()

# Pobierz najnowszy (wg listy, FTP nie zawsze sortuje czasem modyfikacji)
latest_log = sorted(log_files)[-1]
print(f"[INFO] Najnowszy log: {latest_log}")

# Pobierz plik
with open("latest_log.log", "wb") as f:
    ftp.retrbinary(f"RETR {latest_log}", f.write)

ftp.quit()

# --- DEKODOWANIE LOGA ---
with open("latest_log.log", "r", encoding="utf-16le") as f:
    lines = f.readlines()

# --- PARSOWANIE DANYCH ---
import re
from collections import defaultdict

pattern = re.compile(
    r"User:\s+(?P<nick>\w+).*?"
    r"Success:\s+(?P<success>\w+).*?"
    r"Elapsed time:\s+(?P<time>[\d.]+).*?"
    r"Target object:.*?"
    r"Lock type:\s+(?P<lock>\w+)",
    re.DOTALL
)

data = defaultdict(lambda: defaultdict(list))

for line in lines:
    match = pattern.search(line)
    if match:
        nick = match.group("nick")
        lock = match.group("lock")
        success = match.group("success")
        time = float(match.group("time"))

        data[(nick, lock)]['times'].append(time)
        data[(nick, lock)]['success'].append(success == "Yes")

# --- TWORZENIE TABELI ---
rows = []

for (nick, lock), values in data.items():
    total = len(values['success'])
    successes = sum(values['success'])
    fails = total - successes
    avg_time = sum(values['times']) / total if total else 0
    effectiveness = (successes / total * 100) if total else 0

    rows.append([
        nick,
        lock,
        total,
        successes,
        fails,
        f"{effectiveness:.2f}%",
        f"{avg_time:.2f}s"
    ])

df = pd.DataFrame(rows, columns=[
    "Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"
])

# Wyśrodkowanie tekstu
table = df.to_string(index=False, justify="center")

print("[INFO] Tabela gotowa:\n", table)

# --- WYSYŁKA NA DISCORD (WEBHOOK) ---
payload = {
    "content": f"```\n{table}\n```"
}

response = requests.post(WEBHOOK_URL, json=payload)

if response.status_code == 204:
    print("[OK] Wysłano tabelę na Discord.")
else:
    print(f"[ERROR] Nie udało się wysłać na Discord. Status: {response.status_code}")
