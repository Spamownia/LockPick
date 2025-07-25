import ftplib
import re
import statistics
import requests
from flask import Flask
from io import BytesIO
from threading import Timer

# --- KONFIGURACJA FTP I WEBHOOK ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)

def get_ftp_logs():
    print("[DEBUG] Rozpoczynam pobieranie logów z FTP...")
    logs = []
    try:
        ftp = ftplib.FTP()
        print("[DEBUG] Łączenie z FTP...")
        ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_PATH)
        
        files = []
        # Pobieramy listę plików i filtrujemy gameplay_*.log
        try:
            for entry in ftp.mlsd():
                name, facts = entry
                if facts.get("type") == "file" and name.startswith("gameplay_") and name.endswith(".log"):
                    files.append(name)
        except Exception:
            # fallback na LIST jeśli MLSD nie działa
            lines = []
            ftp.retrlines("LIST", lines.append)
            for line in lines:
                parts = line.split(maxsplit=8)
                if len(parts) == 9:
                    name = parts[-1]
                    mode = parts[0]
                    if mode.startswith("-") and name.startswith("gameplay_") and name.endswith(".log"):
                        files.append(name)
        
        print(f"[DEBUG] Znalezione pliki gameplay: {files}")
        
        for filename in files:
            print(f"[INFO] Pobieranie pliku: {filename}")
            bio = BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content_bytes = bio.getvalue()
            try:
                # Dekodujemy UTF-16 LE, zgodnie z podanym kodowaniem logów
                content = content_bytes.decode('utf-16le', errors='ignore')
            except Exception as e:
                print(f"[WARNING] Problem z dekodowaniem pliku {filename}: {e}")
                continue
            logs.append(content)
        
        ftp.quit()
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania logów: {e}")
    print(f"[DEBUG] Liczba pobranych logów: {len(logs)}")
    return logs

def parse_logs(logs):
    # Parsujemy linie w logach do formatu statystyk:
    # Nick, Zamek, Ilość wszystkich prób, Udane, Nieudane, Skuteczność, Średni czas
    # Przykładowy wzorzec (załóżmy) - należy dostosować regex do faktycznego formatu logów:
    #
    # "2025.07.19-15.49.55: Player [Nick] tried to pick lock [Zamek]. Result: Success. Time: 3.45s"
    #
    # UWAGA: Jeśli masz przykładowe linie z logów, proszę podać, aby dokładnie dopasować regex.

    # Ponieważ brak wzoru linii w logu, przyjmijmy następujący przykładowy wzorzec (dane do zmiany zgodnie z logiem!):
    pattern = re.compile(
        r"Player\s+\[(?P<nick>[^\]]+)\].*lock\s+\[(?P<lock>[^\]]+)\].*Result:\s+(?P<result>Success|Failure).*Time:\s+(?P<time>[\d\.]+)s",
        re.IGNORECASE
    )
    
    stats = {}
    
    for log in logs:
        for line in log.splitlines():
            m = pattern.search(line)
            if not m:
                continue
            nick = m.group("nick").strip()
            lock = m.group("lock").strip()
            result = m.group("result").lower()
            time = float(m.group("time"))
            
            key = (nick, lock)
            if key not in stats:
                stats[key] = {
                    "attempts": 0,
                    "success": 0,
                    "fail": 0,
                    "times": []
                }
            stats[key]["attempts"] += 1
            if result == "success":
                stats[key]["success"] += 1
                stats[key]["times"].append(time)
            else:
                stats[key]["fail"] += 1

    # Przetwarzamy statystyki na gotową listę wierszy
    processed = []
    for (nick, lock), data in stats.items():
        attempts = data["attempts"]
        success = data["success"]
        fail = data["fail"]
        success_rate = (success / attempts * 100) if attempts > 0 else 0
        avg_time = statistics.mean(data["times"]) if data["times"] else 0.0
        processed.append({
            "Nick": nick,
            "Zamek": lock,
            "Ilość wszystkich prób": attempts,
            "Udane": success,
            "Nieudane": fail,
            "Skuteczność": f"{success_rate:.1f}%",
            "Średni czas": f"{avg_time:.2f}s"
        })
    return processed

def create_markdown_table(rows):
    # Nagłówki tabeli
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    # Obliczamy szerokości kolumn (max długość tekstu w kolumnie)
    col_widths = {}
    for h in headers:
        max_len = len(h)
        for row in rows:
            max_len = max(max_len, len(str(row[h])))
        col_widths[h] = max_len

    def center_text(text, width):
        text = str(text)
        padding = width - len(text)
        left_pad = padding // 2
        right_pad = padding - left_pad
        return " " * left_pad + text + " " * right_pad

    # Tworzymy nagłówek
    header_line = "|" + "|".join(center_text(h, col_widths[h]) for h in headers) + "|"
    separator_line = "|" + "|".join("-" * col_widths[h] for h in headers) + "|"

    # Tworzymy wiersze
    rows_lines = []
    for row in rows:
        line = "|" + "|".join(center_text(row[h], col_widths[h]) for h in headers) + "|"
        rows_lines.append(line)

    # Łączymy całość
    table_md = "\n".join([header_line, separator_line] + rows_lines)
    return table_md

def send_webhook_table(rows):
    if not rows:
        print("[INFO] Brak danych do wysłania.")
        return
    table_md = create_markdown_table(rows)
    payload = {"content": f"**Statystyki Lockpick:**\n{table_md}"}
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            print("[INFO] Wysłano tabelę statystyk na webhook.")
        else:
            print(f"[ERROR] Błąd wysyłki webhook: {response.status_code} {response.text}")
    except Exception as e:
        print(f"[ERROR] Wyjątek podczas wysyłki webhook: {e}")

@app.route('/')
def index():
    return "Alive"

def main_loop():
    print("[DEBUG] Iteracja pętli głównej...")
    logs = get_ftp_logs()
    if not logs:
        print("[INFO] Brak nowych logów do analizy.")
        return
    stats_rows = parse_logs(logs)
    send_webhook_table(stats_rows)

if __name__ == "__main__":
    def run_periodically(interval, func):
        def wrapper():
            func()
            Timer(interval, wrapper).start()
        wrapper()

    run_periodically(60, main_loop)
    app.run(host='0.0.0.0', port=3000)
