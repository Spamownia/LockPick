import re
import threading
import time
from ftplib import FTP
from io import BytesIO
from flask import Flask
import requests

# --- Konfiguracja ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

CHECK_INTERVAL = 60  # sekund

app = Flask(__name__)

# Słownik na sumaryczne statystyki: { (nick, zamek): { 'all': int, 'success': int, 'fail': int, 'time_sum': float } }
stats = {}

lock = threading.Lock()

# --- Funkcja listująca pliki logów na FTP ---
def list_log_files(ftp):
    ftp.cwd(FTP_LOG_DIR)
    files = []

    def collect_line(line):
        # Parsowanie formatu LIST: permissions, user, group, size, date, name
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            filename = parts[8]
            if re.match(r"gameplay_.*\.log$", filename):
                files.append(filename)

    ftp.retrlines('LIST', callback=collect_line)
    return files

# --- Pobranie pliku logu z FTP (jako bytes) ---
def download_log_file(ftp, filename):
    bio = BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    bio.seek(0)
    return bio.read()

# --- Parsowanie zawartości logu do statystyk ---
# Załóżmy, że w logach linie z danymi mają format:
# "[TIME] Nick=XXX Lock=YYY Result=Success/Fail Duration=ZZ.ZZ"
# Jeśli inny format, podaj proszę, to zmodyfikuję parser.
log_line_regex = re.compile(
    r"Nick=(?P<nick>\S+)\s+Lock=(?P<lock>\S+)\s+Result=(?P<result>Success|Fail)\s+Duration=(?P<duration>[0-9\.]+)"
)

def parse_log_data(content_bytes):
    text = content_bytes.decode('utf-16le')  # dekodowanie UTF-16 LE wg informacji
    local_stats = {}
    for line in text.splitlines():
        m = log_line_regex.search(line)
        if m:
            nick = m.group("nick")
            lock = m.group("lock")
            result = m.group("result")
            duration = float(m.group("duration"))

            key = (nick, lock)
            if key not in local_stats:
                local_stats[key] = {'all': 0, 'success': 0, 'fail': 0, 'time_sum': 0.0}

            local_stats[key]['all'] += 1
            if result == "Success":
                local_stats[key]['success'] += 1
            else:
                local_stats[key]['fail'] += 1
            local_stats[key]['time_sum'] += duration
    return local_stats

# --- Aktualizacja globalnych statystyk ---
def update_stats(new_stats):
    with lock:
        for key, data in new_stats.items():
            if key not in stats:
                stats[key] = data
            else:
                stats[key]['all'] += data['all']
                stats[key]['success'] += data['success']
                stats[key]['fail'] += data['fail']
                stats[key]['time_sum'] += data['time_sum']

# --- Generowanie tekstowej tabeli z danych ---
def generate_table():
    with lock:
        if not stats:
            return "Brak danych do wyświetlenia."

        # Nagłówki
        headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

        # Zbieranie wierszy
        rows = []
        for (nick, lock), data in stats.items():
            all_ = data['all']
            success = data['success']
            fail = data['fail']
            success_rate = (success / all_ * 100) if all_ > 0 else 0
            avg_time = (data['time_sum'] / all_) if all_ > 0 else 0
            rows.append([
                nick,
                lock,
                str(all_),
                str(success),
                str(fail),
                f"{success_rate:.1f}%",
                f"{avg_time:.2f}s"
            ])

        # Oblicz max szerokości kolumn
        columns = list(zip(*([headers] + rows)))
        col_widths = [max(len(str(cell)) for cell in col) for col in columns]

        # Formatowanie linii tabeli z wyśrodkowaniem
        def format_row(row):
            return "| " + " | ".join(f"{cell:^{col_widths[i]}}" for i, cell in enumerate(row)) + " |"

        sep = "|-" + "-|-".join('-' * w for w in col_widths) + "-|"

        table_lines = []
        table_lines.append(format_row(headers))
        table_lines.append(sep)
        for row in rows:
            table_lines.append(format_row(row))

        return "\n".join(table_lines)

# --- Wysyłka tabeli na Discord webhook ---
def send_to_discord(content):
    data = {"content": f"```\n{content}\n```"}
    response = requests.post(DISCORD_WEBHOOK_URL, json=data)
    if response.status_code != 204:
        print(f"[ERROR] Nie udało się wysłać na Discord webhook, status: {response.status_code}, odpowiedź: {response.text}")

# --- Przetwarzanie wszystkich logów z FTP ---
def process_all_logs():
    try:
        with FTP() as ftp:
            ftp.connect(FTP_IP, FTP_PORT, timeout=10)
            ftp.login(FTP_USER, FTP_PASS)

            files = list_log_files(ftp)
            if not files:
                print("[INFO] Nie znaleziono plików logów na FTP.")
                return

            for filename in files:
                content = download_log_file(ftp, filename)
                new_stats = parse_log_data(content)
                update_stats(new_stats)

            print("[INFO] Przetworzono wszystkie dostępne logi.")
    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania listy lub przetwarzania: {e}")

# --- Monitorowanie najnowszego pliku i aktualizacja co 60s ---
def monitor_latest_log():
    last_processed_size = 0
    last_file = None

    while True:
        try:
            with FTP() as ftp:
                ftp.connect(FTP_IP, FTP_PORT, timeout=10)
                ftp.login(FTP_USER, FTP_PASS)
                ftp.cwd(FTP_LOG_DIR)

                files = list_log_files(ftp)
                if not files:
                    print("[INFO] Brak plików logów do monitorowania.")
                    time.sleep(CHECK_INTERVAL)
                    continue

                # wybierz najnowszy plik (alfabetycznie, przy założeniu, że najnowszy ma największą nazwę)
                newest_file = sorted(files)[-1]

                if newest_file != last_file:
                    last_file = newest_file
                    last_processed_size = 0

                # pobierz plik
                bio = BytesIO()
                ftp.retrbinary(f"RETR {newest_file}", bio.write)
                bio.seek(0)
                content_bytes = bio.read()

                if len(content_bytes) > last_processed_size:
                    new_content = content_bytes[last_processed_size:]
                    new_stats = parse_log_data(new_content)
                    update_stats(new_stats)
                    last_processed_size = len(content_bytes)

                    # Wyślij tabelę na Discorda
                    table_text = generate_table()
                    send_to_discord(table_text)

            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            print(f"[ERROR] Błąd podczas monitorowania logów: {e}")
            time.sleep(CHECK_INTERVAL)

# --- Uruchomienie monitorowania w osobnym wątku ---
def start_monitor_thread():
    thread = threading.Thread(target=monitor_latest_log, daemon=True)
    thread.start()

# --- Endpoint testowy ---
@app.route("/")
def index():
    return "Lockpick logs service is running."

# --- Główna część ---
if __name__ == "__main__":
    print("🔁 Uruchamianie pełnego przetwarzania logów...")
    process_all_logs()
    start_monitor_thread()
    print("     ==> Your service is live 🎉")
    app.run(host="0.0.0.0", port=10000)
