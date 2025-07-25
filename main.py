import ftplib
from io import StringIO
import time
import threading
import requests
from flask import Flask, jsonify

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)

def get_ftp_logs():
 print("[DEBUG] Rozpoczynam pobieranie logów z FTP...")
 logs = []
 try:
  ftp = ftplib.FTP()
  print("[DEBUG] Utworzono instancję FTP, próbuję connect()...")
  ftp.connect(FTP_HOST, FTP_PORT)
  print("[DEBUG] connect() zakończone, próbuję login()...")
  ftp.login(FTP_USER, FTP_PASS)
  print("[DEBUG] login() zakończone, próbuję cwd()...")
  ftp.cwd("/SCUM/Saved/SaveFiles/Logs")
  print("[DEBUG] cwd() zakończone, próbuję nlst()...")
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
 import re
 stats = {}
 pattern = re.compile(r"Lockpick \[(\w+)\] took ([\d\.]+) seconds")
 for log in logs:
  for line in log.splitlines():
   m = pattern.search(line)
   if m:
    key = m.group(1)
    elapsed = m.group(2).rstrip(".")
    if key not in stats:
     stats[key] = {"count": 0, "times": []}
    try:
     stats[key]["times"].append(float(elapsed))
     stats[key]["count"] += 1
    except ValueError:
     print(f"[WARNING] Nieprawidłowa wartość czasu: '{elapsed}' w linii: {line}")
 return stats

def send_webhook(stats):
 for key, data in stats.items():
  avg_time = sum(data["times"]) / len(data["times"]) if data["times"] else 0
  message = f"Lockpick {key}: {data['count']} attempts, average time: {avg_time:.2f}s"
  payload = {"content": message}
  try:
   response = requests.post(WEBHOOK_URL, json=payload)
   if response.status_code != 204:
    print(f"[ERROR] Webhook send failed: {response.status_code} {response.text}")
  except Exception as e:
   print(f"[ERROR] Exception sending webhook: {e}")

def main_loop():
 while True:
  print("[DEBUG] Iteracja pętli głównej...")
  logs = get_ftp_logs()
  if logs:
   stats = parse_lockpicks(logs)
   if stats:
    send_webhook(stats)
   else:
    print("[INFO] Brak nowych wpisów do przetworzenia.")
  else:
   print("[INFO] Brak nowych wpisów.")
  time.sleep(60)

@app.route("/")
def index():
 return "Alive"

if __name__ == "__main__":
 # Uruchomienie pętli głównej w wątku, by Flask mógł działać równolegle
 threading.Thread(target=main_loop, daemon=True).start()
 app.run(host="0.0.0.0", port=3000)
