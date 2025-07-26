# --- AUTOMATYCZNA INSTALACJA WYMAGANYCH BIBLIOTEK ---
import subprocess
import sys
import os
import re
import pandas as pd
from ftplib import FTP
from io import BytesIO
import requests

# --- INSTALACJA ---
try:
    import pandas as pd
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
    import pandas as pd

# --- KONFIGURACJA FTP ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/Scum/Saved/Logs/"

# --- KONFIGURACJA WEBHOOKA ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- POBIERZ NAJNOWSZE PLIKI LOGÓW Z FTP ---
def download_latest_log_files():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    files = []
    ftp.retrlines("NLST", files.append)

    gameplay_logs = sorted([f for f in files if f.startswith("gameplay_") and f.endswith(".log")])
    print(f"[INFO] Znaleziono {len(gameplay_logs)} plików gameplay_*.log")

    log_contents = []
    for filename in gameplay_logs[-30:]:  # ostatnie 30
        print(f"[INFO] Pobieranie pliku: {filename}")
        with BytesIO() as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
            f.seek(0)
            content = f.read().decode("utf-8", errors="ignore")
            log_contents.append(content)

    ftp.quit()
    return "\n".join(log_contents)

# --- PRZETWARZANIE LOGÓW ---
def parse_gameplay_logs(log_text):
    pattern = re.compile(
        r"(?P<timestamp>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): "
        r"(?P<killer>.*?)\[(?P<weapon>[^\]]*)\] killed (?P<victim>.*?) at distance (?P<distance>[\d\.]+)m"
    )

    data = []
    for match in pattern.finditer(log_text):
        data.append({
            "Czas": match.group("timestamp").replace("-", " "),
            "Zabójca": match.group("killer").strip(),
            "Broń": match.group("weapon").strip(),
            "Ofiara": match.group("victim").strip(),
            "Dystans": float(match.group("distance"))
        })

    return pd.DataFrame(data)

# --- GENEROWANIE TABELI HTML ---
def dataframe_to_html_table(df):
    df_str = df.astype(str)
    col_widths = df_str.applymap(len)
    col_name_widths = pd.DataFrame([df_str.columns.str.len().tolist()], columns=df_str.columns)
    max_lengths = pd.concat([col_widths, col_name_widths], axis=0).max()

    html = "<table border='1' style='border-collapse: collapse; font-family: monospace; text-align: center;'>\n"
    html += "  <tr>" + "".join(
        f"<th style='padding:4px'>{col}</th>" for col in df.columns
    ) + "</tr>\n"

    for _, row in df.iterrows():
        html += "  <tr>" + "".join(
            f"<td style='padding:4px'>{str(val).ljust(max_lengths[col])}</td>" for col, val in row.items()
        ) + "</tr>\n"
    html += "</table>"
    return html

# --- WYSYŁANIE NA WEBHOOK DISCORDA ---
def send_to_discord(webhook_url, html_table):
    response = requests.post(webhook_url, json={"content": html_table})
    if response.status_code == 204:
        print("[OK] Tabela wysłana na Discord.")
    else:
        print(f"[BŁĄD] Nie udało się wysłać na Discord: {response.status_code}, {response.text}")

# --- GŁÓWNA FUNKCJA ---
if __name__ == "__main__":
    logs = download_latest_log_files()
    df = parse_gameplay_logs(logs)

    if df.empty:
        print("[INFO] Brak danych do wysłania.")
    else:
        tabela_html = dataframe_to_html_table(df)
        send_to_discord(WEBHOOK_URL, tabela_html)
