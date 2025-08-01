import os
import io
import re
import pandas as pd
import requests
from tabulate import tabulate
from ftplib import FTP

# == KONFIGURACJA FTP ==
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

# == WEBHOOK ==
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

def connect_ftp():
    print("[DEBUG] Łączenie z FTP...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    print("[DEBUG] Połączono z FTP.")
    return ftp

def fetch_log_files(ftp):
    print("[DEBUG] Pobieranie listy plików logów...")
    files = []
    ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
    log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]
    print(f"[DEBUG] Znaleziono {len(log_files)} plików gameplay_*.log")
    return log_files

def parse_log_content(content):
    data = []
    for line in content.splitlines():
        if "[LogMinigame]" not in line:
            continue
        match = re.search(r"User: (.*?) \[.*?\] Lock: (.*?) \[.*?\] Success: (Yes|No).*?Elapsed time: ([\d.]+)", line)
        if match:
            nick = match.group(1)
            lock = match.group(2)
            success = match.group(3) == "Yes"
            elapsed = float(match.group(4))
            data.append((nick, lock, success, elapsed))
    return data

def analyze_data(data):
    if not data:
        print("[DEBUG] Brak danych do analizy.")
        return None

    df = pd.DataFrame(data, columns=["Nick", "Zamek", "Sukces", "Czas"])
    grouped = df.groupby(["Nick", "Zamek"])

    results = []
    for (nick, lock), group in grouped:
        total = len(group)
        success_count = group["Sukces"].sum()
        fail_count = total - success_count
        accuracy = round((success_count / total) * 100, 1)
        avg_time = round(group["Czas"].mean(), 2)
        results.append((nick, lock, total, success_count, fail_count, f"{accuracy}%", f"{avg_time}s"))

    results.sort(key=lambda x: (x[0].lower(), x[1].lower()))
    return results

def send_table_to_discord(data):
    if not data:
        print("[DEBUG] Brak danych do wysłania.")
        return

    headers = ["Nick", "Rodzaj zamka", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = tabulate(data, headers=headers, tablefmt="github", stralign="center", numalign="center")
    print("[DEBUG] Tabela do wysyłki:\n", table)

    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[DEBUG] Wysłano na webhook, status: {response.status_code}")

def main():
    ftp = connect_ftp()
    log_files = fetch_log_files(ftp)

    all_data = []
    for filename in log_files:
        print(f"[DEBUG] Przetwarzanie: {filename}")
        buffer = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", buffer.write)
        buffer.seek(0)
        content = buffer.read().decode("utf-16-le", errors="ignore")
        parsed = parse_log_content(content)
        all_data.extend(parsed)

    ftp.quit()
    print(f"[DEBUG] Łącznie wpisów: {len(all_data)}")
    analyzed = analyze_data(all_data)
    send_table_to_discord(analyzed)

if __name__ == "__main__":
    main()
