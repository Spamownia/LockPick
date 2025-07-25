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
import re
from collections import defaultdict

# --- DANE FTP I WEBHOOK ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- POŁĄCZENIE Z FTP I POBRANIE WSZYSTKICH gameplay_*.log ---
ftp = FTP()
ftp.connect(FTP_HOST, FTP_PORT)
ftp.login(FTP_USER, FTP_PASS)
ftp.cwd(LOG_DIR)

# Pobranie listy plików przez LIST (ponieważ NLST nie działa)
files = []
ftp.retrlines('LIST', files.append)

# Filtrowanie nazw plików zawierających "gameplay_"
gameplay_logs = []
for line in files:
    parts = line.split()
    filename = parts[-1]
    if filename.startswith("gameplay_") and filename.endswith(".log"):
        gameplay_logs.append(filename)

print(f"[INFO] Znaleziono {len(gameplay_logs)} plików gameplay_*.log")

# Pobranie i odczytanie wszystkich logów
all_lines = []

for log_file in gameplay_logs:
    print(f"[INFO] Pobieranie pliku: {log_file}")
    local_filename = f"tmp_{log_file}"

    with open(local_filename, "wb") as f:
        ftp.retrbinary(f"RETR {log_file}", f.write)

    with open(local_filename, "r", encoding="utf-16le") as f:
        lines = f.readlines()
        all_lines.extend(lines)

    os.remove(local_filename)

ftp.quit()

# --- PARSOWANIE DANYCH ---
pattern = re.compile(
    r"User:\s+(?P<nick>\w+).*?"
    r"Success:\s+(?P<success>\w+).*?"
    r"Elapsed time:\s+(?P<time>[\d.]+).*?"
    r"Lock type:\s+(?P<lock>\w+)",
    re.DOTALL
)

data = defaultdict(lambda: defaultdict(list))

for line in all_lines:
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

# Wyśrodkowanie tekstu w pandas.to_string()
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
