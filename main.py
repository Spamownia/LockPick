import ftplib
import re
import statistics
import requests
from flask import Flask

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
        ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
        print("[DEBUG] connect() zakończone, próbuję login()...")
        ftp.login(FTP_USER, FTP_PASS)
        print("[DEBUG] login() zakończone, próbuję cwd()...")
        ftp.cwd("/SCUM/Saved/SaveFiles/Logs")
        print("[DEBUG] cwd() zakończone, próbuję pobrać listę plików...")

        files = []
        # Najpierw próbujemy mlsd() - bardziej nowoczesne i stabilne
        try:
            for entry in ftp.mlsd():
                name, facts = entry
                if facts.get("type") == "file" and name.endswith(".log"):
                    files.append(name)
            print(f"[DEBUG] Pliki pobrane przez MLSD: {files}")
        except (ftplib.error_perm, AttributeError) as e:
            print(f"[WARNING] MLSD nie działa ({e}), próbuję LIST...")
            # Fallback: ręczne parsowanie LIST
            lines = []
            ftp.retrlines("LIST", lines.append)
            for line in lines:
                parts = line.split(maxsplit=8)
                if len(parts) == 9:
                    name = parts[-1]
                    mode = parts[0]
                    if mode.startswith("-") and name.endswith(".log"):
                        files.append(name)
            print(f"[DEBUG] Pliki pobrane przez LIST: {files}")

        for filename in files:
            print(f"[INFO] Downloading: {filename}")
            from io import StringIO
            sio = StringIO()
            ftp.retrlines(f"RETR {filename}", lambda line: sio.write(line + "\n"))
            logs.append(sio.getvalue())
        ftp.quit()
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania logów: {e}")
    print(f"[DEBUG] Liczba pobranych logów: {len(logs)}")
    return logs

def parse_lockpicks(logs):
    stats = {}
    pattern = re.compile(r"Lockpick succeeded in ([\d\.]+) seconds with (.+)")
    for log in logs:
        for line in log.splitlines():
            m = pattern.search(line)
            if m:
                elapsed = m.group(1).rstrip('.')  # usuń kropkę jeśli występuje
                weapon = m.group(2)
                try:
                    elapsed_float = float(elapsed)
                except ValueError:
                    print(f"[WARNING] Niepoprawna wartość czasu: '{elapsed}', pomijam linię.")
                    continue
                key = weapon
                if key not in stats:
                    stats[key] = {"times": []}
                stats[key]["times"].append(elapsed_float)
    for key in stats:
        times = stats[key]["times"]
        stats[key]["count"] = len(times)
        stats[key]["min"] = min(times)
        stats[key]["max"] = max(times)
        stats[key]["avg"] = statistics.mean(times)
        stats[key]["median"] = statistics.median(times)
    return stats

def send_webhook(stats):
    if not stats:
        print("[INFO] Brak statystyk do wysłania.")
        return
    for weapon, data in stats.items():
        content = (
            f"**Statystyki lockpicków dla:** {weapon}\n"
            f"Liczba prób: {data['count']}\n"
            f"Min czas: {data['min']:.2f} s\n"
            f"Max czas: {data['max']:.2f} s\n"
            f"Średni czas: {data['avg']:.2f} s\n"
            f"Mediana: {data['median']:.2f} s\n"
        )
        payload = {"content": content}
        try:
            response = requests.post(WEBHOOK_URL, json=payload)
            if response.status_code == 204:
                print(f"[INFO] Wysłano statystyki dla {weapon}")
            else:
                print(f"[ERROR] Błąd wysyłki webhook dla {weapon}: {response.status_code} {response.text}")
        except Exception as e:
            print(f"[ERROR] Wyjątek podczas wysyłki webhook: {e}")

@app.route('/')
def index():
    return "Alive"

def main_loop():
    print("[DEBUG] Iteracja pętli głównej...")
    logs = get_ftp_logs()
    if not logs:
        print("[INFO] Brak nowych wpisów.")
        return
    stats = parse_lockpicks(logs)
    send_webhook(stats)

if __name__ == "__main__":
    from threading import Timer

    def run_periodically(interval, func):
        def wrapper():
            func()
            Timer(interval, wrapper).start()
        wrapper()

    run_periodically(60, main_loop)
    app.run(host='0.0.0.0', port=3000)
