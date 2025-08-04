import os
import re
import threading
import time
from ftplib import FTP
from io import BytesIO
from flask import Flask
import requests

# --- Konfiguracja FTP i webhooka ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Flask app ---
app = Flask(__name__)

# --- Globalna zmienna na statystyki ---
stats = {}

stats_lock = threading.Lock()

# --- Funkcje pomocnicze ---

def ftp_connect():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=20)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def list_log_files(ftp):
    ftp.cwd(FTP_LOG_DIR)
    files = ftp.nlst()
    # Filtrujemy nazwy gameplay_*.log
    log_files = [f for f in files if re.match(r"gameplay_.*\.log$", f)]
    return log_files

def download_log(ftp, filename):
    bio = BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    bio.seek(0)
    content = bio.read()
    # Dekodujemy UTF-16 LE
    text = content.decode("utf-16le", errors="ignore")
    return text

def parse_log_content(text):
    """
    Parsuje log do statystyk.
    Założenie: Każdy wpis w logu zawiera: Nick, Zamek, sukces (True/False), czas (sekundy)
    Przykład linii loga (należy dostosować do rzeczywistego formatu!):

    [timestamp] Nick=Anu; Zamek=Advanced; Result=False; Time=12.67
    [timestamp] Nick=Szturman; Zamek=VeryEasy; Result=True; Time=5.78

    Funkcja zwraca słownik:
    {
        (Nick, Zamek): {"all": int, "success": int, "fail": int, "total_time": float}
    }
    """

    pattern = re.compile(
        r"Nick=(?P<nick>[^;]+);\s*Zamek=(?P<zamek>[^;]+);\s*Result=(?P<result>True|False);\s*Time=(?P<time>[0-9.]+)",
        re.IGNORECASE,
    )
    local_stats = {}

    for line in text.splitlines():
        m = pattern.search(line)
        if not m:
            continue
        nick = m.group("nick").strip()
        zamek = m.group("zamek").strip()
        result = m.group("result").strip().lower() == "true"
        czas = float(m.group("time"))

        key = (nick, zamek)
        if key not in local_stats:
            local_stats[key] = {"all": 0, "success": 0, "fail": 0, "total_time": 0.0}
        local_stats[key]["all"] += 1
        if result:
            local_stats[key]["success"] += 1
        else:
            local_stats[key]["fail"] += 1
        local_stats[key]["total_time"] += czas

    return local_stats

def merge_stats(base, new):
    for key, vals in new.items():
        if key not in base:
            base[key] = vals.copy()
        else:
            base[key]["all"] += vals["all"]
            base[key]["success"] += vals["success"]
            base[key]["fail"] += vals["fail"]
            base[key]["total_time"] += vals["total_time"]

def format_table(stats_dict):
    # Nagłówki
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    # Budujemy wiersze z danymi
    rows = []
    for (nick, zamek), v in sorted(stats_dict.items(), key=lambda x: (x[0][0].lower(), x[0][1].lower())):
        all_ = v["all"]
        success = v["success"]
        fail = v["fail"]
        skut = (success / all_ * 100) if all_ > 0 else 0.0
        sr_czas = (v["total_time"] / all_) if all_ > 0 else 0.0

        rows.append([
            nick,
            zamek,
            str(all_),
            str(success),
            str(fail),
            f"{skut:.1f}%",
            f"{sr_czas:.2f}s"
        ])

    # Obliczamy szerokości kolumn (max długość pola)
    col_widths = []
    for col_i in range(len(headers)):
        max_len = len(headers[col_i])
        for row in rows:
            max_len = max(max_len, len(row[col_i]))
        col_widths.append(max_len)

    # Funkcja do wyśrodkowania tekstu
    def center(text, width):
        return text.center(width)

    # Budujemy tabelę jako string z separatorami pipe "|"
    sep = "|"
    sep_line = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"

    lines = []
    lines.append(sep_line)
    # Nagłówek
    header_line = sep + sep.join(" " + center(headers[i], col_widths[i]) + " " for i in range(len(headers))) + sep
    lines.append(header_line)
    lines.append(sep_line.replace("-", "="))  # oddzielnik po nagłówku

    # Wiersze danych
    for row in rows:
        line = sep + sep.join(" " + center(row[i], col_widths[i]) + " " for i in range(len(row))) + sep
        lines.append(line)
    lines.append(sep_line)

    # Zwracamy całość jako pojedynczy string w bloku kodu do Discorda
    table_text = "\n".join(lines)
    return f"```\n{table_text}\n```"

def send_to_discord(text):
    payload = {"content": text}
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code >= 300:
            print(f"[WARN] Discord webhook zwrócił status {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[ERROR] Błąd wysyłki do Discord webhook: {e}")

# --- Główna funkcja przetwarzania wszystkich logów ---
def process_all_logs():
    global stats
    print("🔁 Uruchamianie pełnego przetwarzania logów...")

    try:
        ftp = ftp_connect()
    except Exception as e:
        print(f"[ERROR] Nie udało się połączyć z FTP: {e}")
        return

    try:
        log_files = list_log_files(ftp)
        if not log_files:
            print("[INFO] Brak logów do przetworzenia na FTP.")
            ftp.quit()
            return

        new_stats = {}

        for filename in log_files:
            try:
                content = download_log(ftp, filename)
                parsed = parse_log_content(content)
                merge_stats(new_stats, parsed)
            except Exception as e:
                print(f"[WARN] Błąd przy przetwarzaniu {filename}: {e}")

        with stats_lock:
            stats = new_stats

        print("[INFO] Przetwarzanie logów zakończone.")
        ftp.quit()
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania listy lub przetwarzania: {e}")

def process_latest_log():
    global stats

    try:
        ftp = ftp_connect()
        log_files = list_log_files(ftp)
        if not log_files:
            ftp.quit()
            print("[INFO] Brak logów do przetworzenia w monitoringu.")
            return
        # Najnowszy plik wg nazwy (zakładamy nazwy rosnące)
        latest_log = sorted(log_files)[-1]
        content = download_log(ftp, latest_log)
        parsed = parse_log_content(content)

        with stats_lock:
            merge_stats(stats, parsed)
            table_text = format_table(stats)

        send_to_discord(table_text)
        ftp.quit()
        print(f"[INFO] Wysłano aktualizację tabeli dla pliku {latest_log}.")
    except Exception as e:
        print(f"[ERROR] Błąd podczas przetwarzania najnowszego loga: {e}")

def monitor_new_logs():
    while True:
        try:
            process_latest_log()
        except Exception as e:
            print(f"[ERROR] Błąd w monitoringu logów: {e}")
        time.sleep(60)

# --- Uruchomienie wątku monitorującego po starcie ---
def start_monitor_thread():
    thread = threading.Thread(target=monitor_new_logs, daemon=True)
    thread.start()

@app.route("/")
def index():
    return "Serwis działa."

if __name__ == "__main__":
    process_all_logs()
    start_monitor_thread()
    app.run(host="0.0.0.0", port=10000)
