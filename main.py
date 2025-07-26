# --- AUTOMATYCZNA INSTALACJA WYMAGANYCH BIBLIOTEK ---
import subprocess, sys

for pkg in ["requests", "pandas"]:
    try:
        __import__(pkg)
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

# --- POŁĄCZENIE Z FTP I POBRANIE gameplay_*.log ---
ftp = FTP()
ftp.connect(FTP_HOST, FTP_PORT)
ftp.login(FTP_USER, FTP_PASS)
ftp.cwd(LOG_DIR)

files = []
ftp.retrlines('LIST', files.append)

gameplay_logs = [
    line.split()[-1] for line in files
    if line.split()[-1].startswith("gameplay_") and line.split()[-1].endswith(".log")
]

print(f"[INFO] Znaleziono {len(gameplay_logs)} plików gameplay_*.log")

# --- POBRANIE I ODCZYT LOGÓW ---
all_lines = []

for log_file in gameplay_logs:
    print(f"[INFO] Pobieranie pliku: {log_file}")
    local_filename = f"tmp_{log_file}"

    with open(local_filename, "wb") as f:
        ftp.retrbinary(f"RETR {log_file}", f.write)

    with open(local_filename, "r", encoding="utf-16le") as f:
        all_lines.extend(f.readlines())

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
        if lock == "Easy":
            lock = "Basic"
        success = match.group("success")
        time_str = match.group("time").rstrip(".")
        try:
            time = float(time_str)
        except ValueError:
            print(f"[WARNING] Niepoprawna wartość czasu: '{time_str}'. Pomijam wpis.")
            continue

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

if not rows:
    print("[INFO] Brak danych do wysłania.")
else:
    df = pd.DataFrame(rows, columns=[
        "Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"
    ])

    # Kolejność zamków
    lock_order = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4}
    df["Zamek_kolejnosc"] = df["Zamek"].map(lock_order)
    df = df.sort_values(by=["Nick", "Zamek_kolejnosc"]).drop(columns=["Zamek_kolejnosc"])

    # --- WYŚRODKOWANIE KOMÓREK I NAGŁÓWKÓW ---
    df_str = df.astype(str)
    max_lengths = df_str.applymap(len).combine(df_str.columns.to_series().apply(len), max)

    for col in df_str.columns:
        df_str[col] = df_str[col].apply(lambda val: val.center(max_lengths[col]))

    headers = [col.center(max_lengths[col]) for col in df_str.columns]
    table = pd.DataFrame([headers] + df_str.values.tolist())
    table_text = table.to_string(index=False, header=False)

    print("[INFO] Tabela gotowa:\n", table_text)

    # --- WYSYŁKA NA DISCORD ---
    payload = {
        "content": f"```\n{table_text}\n```"
    }

    response = requests.post(WEBHOOK_URL, json=payload)

    if response.status_code == 204:
        print("[OK] Wysłano tabelę na Discord.")
    else:
        print(f"[ERROR] Nie udało się wysłać na Discord. Status: {response.status_code}")
