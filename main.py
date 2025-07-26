import re
import pandas as pd
import requests
import os
from ftplib import FTP
from io import BytesIO
from collections import defaultdict
from flask import Flask

# --- Konfiguracja ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def download_latest_log_files():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    listing = []
    ftp.retrlines("LIST", listing.append)

    files = [line.split()[-1] for line in listing]
    gameplay_logs = sorted([f for f in files if f.startswith("gameplay_") and f.endswith(".log")])
    print(f"[INFO] Znaleziono {len(gameplay_logs)} plików gameplay_*.log")

    log_contents = []
    for filename in gameplay_logs[-30:]:
        print(f"[INFO] Pobieranie pliku: {filename}")
        with BytesIO() as f:
            ftp.retrbinary(f"RETR " + filename, f.write)
            f.seek(0)
            content = f.read().decode("utf-8", errors="ignore")
            log_contents.append(content)

    ftp.quit()
    return "\n".join(log_contents)

def parse_lockpick_attempts(log_data):
    pattern = r'(?P<time>\d+\.\d+).*?(?P<nickname>.+?) started lockpicking a lock \(Basic\)'
    matches = re.finditer(pattern, log_data)
    attempts = defaultdict(int)

    for match in matches:
        nickname = match.group("nickname").strip()
        attempts[nickname] += 1

    return attempts

def send_lockpick_table_to_discord(lockpick_data):
    if not lockpick_data:
        print("[INFO] Brak danych do wysłania.")
        return

    df = pd.DataFrame(lockpick_data.items(), columns=["Nickname", "Attempts"])
    df = df.sort_values(by="Attempts", ascending=False).reset_index(drop=True)

    # Wyśrodkowanie zawartości i nagłówków w HTML
    df_html = df.to_html(index=False, escape=False, border=0, classes="centered", justify="center")

    html_message = f"""
    <html>
    <head>
        <style>
            table.centered {{
                border-collapse: collapse;
                margin-left: auto;
                margin-right: auto;
            }}
            table.centered th, table.centered td {{
                border: 1px solid #ccc;
                padding: 8px;
                text-align: center;
            }}
            table.centered th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
        </style>
    </head>
    <body>
        <h3 style="text-align:center;">🔐 Lockpick Attempts (Basic)</h3>
        {df_html}
    </body>
    </html>
    """

    payload = {
        "content": None,
        "embeds": [{
            "title": "Lockpick Stats",
            "description": "Tabela prób podważania zamków typu **Basic**",
            "type": "rich"
        }],
        "attachments": []
    }

    response = requests.post(WEBHOOK_URL, json={"content": html_message})
    if response.status_code == 204:
        print("[INFO] Tabela wysłana do Discord Webhook.")
    else:
        print(f"[ERROR] Błąd wysyłania do Discord: {response.status_code} - {response.text}")

if __name__ == "__main__":
    log_data = download_latest_log_files()
    lockpick_data = parse_lockpick_attempts(log_data)
    send_lockpick_table_to_discord(lockpick_data)
    app.run(host='0.0.0.0', port=3000)
