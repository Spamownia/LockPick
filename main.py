import os
import ftplib
import time
import pandas as pd
import requests
from io import StringIO
from flask import Flask

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

def get_ftp_logs():
    print("[DEBUG] Rozpoczynam pobieranie logów z FTP...")
    logs = []
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        print("[DEBUG] Połączono z FTP")
        ftp.cwd("/SCUM/Saved/SaveFiles/Logs")
        files = ftp.nlst()
        print(f"[DEBUG] Pliki na FTP: {files}")

        for filename in files:
            if filename.endswith(".log"):
                print(f"[INFO] Downloading: {filename}")
                sio = StringIO()
                ftp.retrlines(f"RETR {filename}", lambda line: sio.write(line + "\n"))
                logs.append(sio.getvalue())
        ftp.quit()
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania logów: {e}")
    print(f"[DEBUG] Liczba pobranych logów: {len(logs)}")
    return logs

def parse_lockpicks(logs):
    print("[DEBUG] Parsowanie logów lockpick...")
    stats = {}
    for log in logs:
        lines = log.splitlines()
        for line in lines:
            if "lockpick" in line:
                parts = line.split()
                if len(parts) >= 4:
                    player = parts[2]
                    elapsed = parts[-1].replace(".", "").replace(",", ".")
                    key = player
                    if key not in stats:
                        stats[key] = {"times": []}
                    try:
                        stats[key]["times"].append(float(elapsed))
                    except ValueError:
                        print(f"[WARN] Nieprawidłowa wartość czasu: '{elapsed}' w linii: {line}")
    print(f"[DEBUG] Liczba graczy w statystykach: {len(stats)}")
    return stats

def generate_table(stats):
    print("[DEBUG] Generowanie tabeli...")
    if not stats:
        print("[INFO] Brak danych do wygenerowania tabeli.")
        return None
    df = pd.DataFrame([
        {"Player": player, "Attempts": len(data["times"]), "Avg Time": sum(data["times"]) / len(data["times"])}
        for player, data in stats.items()
    ])
    df.sort_values(by=["Attempts"], ascending=False, inplace=True)
    table = df.to_markdown(index=False)
    print("[DEBUG] Wygenerowana tabela:\n" + table)
    return table

def send_to_webhook(content):
    if not content:
        print("[INFO] Brak treści do wysłania na webhook.")
        return
    data = {"content": f"```\n{content}\n```"}
    try:
        response = requests.post(WEBHOOK_URL, json=data)
        if response.status_code == 204:
            print("[INFO] Tabela wysłana na webhook.")
        else:
            print(f"[ERROR] Błąd wysyłki na webhook: {response.status_code} {response.text}")
    except Exception as e:
        print(f"[ERROR] Wyjątek podczas wysyłki na webhook: {e}")

def main_loop():
    while True:
        print("[DEBUG] Iteracja pętli głównej...")
        logs = get_ftp_logs()
        if not logs:
            print("[INFO] Brak nowych logów.")
        else:
            stats = parse_lockpicks(logs)
            table = generate_table(stats)
            if table:
                send_to_webhook(table)
            else:
                print("[INFO] Tabela nie została wygenerowana.")
        time.sleep(60)

# Flask dla uptimerobot
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    from threading import Thread
    t = Thread(target=main_loop)
    t.start()
    app.run(host='0.0.0.0', port=3000)
