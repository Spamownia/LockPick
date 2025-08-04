import ftplib
import io
import re
import threading
import time
from collections import defaultdict
from flask import Flask, Response
import requests

# FTP dane
FTP_HOST = '176.57.174.10'
FTP_PORT = 50021
FTP_USER = 'gpftp37275281717442833'
FTP_PASS = 'LXNdGShY'
FTP_LOG_PATH = '/SCUM/Saved/SaveFiles/Logs/'

# Discord webhook
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3'

# Lockpicking zamki kolejność sortowania
LOCK_ORDER = ['VeryEasy', 'Basic', 'Medium', 'Advanced', 'DialLock']

# Regex do parsowania linii loga lockpickingu
LOG_PATTERN = re.compile(
    r'User: (?P<nick>.+?) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[0-9.]+)\. Failed attempts: (?P<fail>\d+)\. .*?Lock type: (?P<lock>\w+)\.'
)

app = Flask(__name__)

# Dane globalne
stats_lock = threading.Lock()
stats = defaultdict(lambda: defaultdict(lambda: {
    'all': 0,
    'success': 0,
    'fail': 0,
    'total_time': 0.0
}))

last_processed_line = 0
last_log_filename = None

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)
    return ftp

def list_logs(ftp):
    lines = []
    ftp.retrlines('LIST', lines.append)
    files = []
    for line in lines:
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            filename = parts[8]
            if filename.startswith('gameplay_') and filename.endswith('.log'):
                files.append(filename)
    return files

def parse_line(line):
    match = LOG_PATTERN.search(line)
    if not match:
        return None
    nick = match.group('nick')
    success = match.group('success') == 'Yes'
    elapsed = float(match.group('time'))
    fail = int(match.group('fail'))
    lock = match.group('lock')
    return nick, lock, success, elapsed, fail

def update_stats(nick, lock, success, elapsed, fail):
    with stats_lock:
        entry = stats[nick][lock]
        entry['all'] += 1
        if success:
            entry['success'] += 1
        else:
            entry['fail'] += 1
        entry['total_time'] += elapsed

def fetch_and_parse_log(ftp, filename):
    global last_processed_line
    try:
        bio = io.BytesIO()
        ftp.retrbinary(f'RETR {filename}', bio.write)
        bio.seek(0)
        content = bio.read().decode('utf-16le', errors='ignore')
    except Exception as e:
        print(f"[ERROR] Pobieranie lub dekodowanie loga {filename} nie powiodło się: {e}")
        return 0

    lines = content.splitlines()
    processed = 0
    for line in lines:
        res = parse_line(line)
        if res:
            nick, lock, success, elapsed, fail = res
            update_stats(nick, lock, success, elapsed, fail)
            processed += 1
    return processed

def fetch_and_parse_log_incremental(ftp, filename, from_line):
    try:
        bio = io.BytesIO()
        ftp.retrbinary(f'RETR {filename}', bio.write)
        bio.seek(0)
        content = bio.read().decode('utf-16le', errors='ignore')
    except Exception as e:
        print(f"[ERROR] Pobieranie lub dekodowanie loga {filename} nie powiodło się: {e}")
        return 0, from_line

    lines = content.splitlines()
    new_lines = lines[from_line:]
    processed = 0
    for line in new_lines:
        res = parse_line(line)
        if res:
            nick, lock, success, elapsed, fail = res
            update_stats(nick, lock, success, elapsed, fail)
            processed += 1
    return processed, len(lines)

def generate_table():
    with stats_lock:
        rows = []
        for nick in sorted(stats.keys()):
            for lock in LOCK_ORDER:
                if lock in stats[nick]:
                    entry = stats[nick][lock]
                    all_ = entry['all']
                    success = entry['success']
                    fail = entry['fail']
                    accuracy = (success / all_ * 100) if all_ > 0 else 0.0
                    avg_time = (entry['total_time'] / all_) if all_ > 0 else 0.0
                    rows.append((
                        nick,
                        lock,
                        all_,
                        success,
                        fail,
                        f"{accuracy:.1f}%",
                        f"{avg_time:.2f}s"
                    ))

        headers = ['Nick', 'Zamek', 'Wszystkie', 'Udane', 'Nieudane', 'Skuteczność', 'Średni czas']
        columns = list(zip(*([headers] + rows))) if rows else [headers]

        col_widths = [max(len(str(item)) for item in col) for col in columns]

        def center(text, width):
            text = str(text)
            space = width - len(text)
            left = space // 2
            right = space - left
            return ' ' * left + text + ' ' * right

        sep_line = '|' + '|'.join(['-' * w for w in col_widths]) + '|'
        header_line = '|' + '|'.join(center(h, w) for h, w in zip(headers, col_widths)) + '|'
        row_lines = ['|' + '|'.join(center(c, w) for c, w in zip(row, col_widths)) + '|' for row in rows]

        return '\n'.join([header_line, sep_line] + row_lines)

def generate_short_table():
    with stats_lock:
        rows = []
        for nick in sorted(stats.keys()):
            for lock in LOCK_ORDER:
                if lock in stats[nick]:
                    entry = stats[nick][lock]
                    all_ = entry['all']
                    success = entry['success']
                    accuracy = (success / all_ * 100) if all_ > 0 else 0.0
                    avg_time = (entry['total_time'] / all_) if all_ > 0 else 0.0
                    rows.append((
                        nick,
                        lock,
                        f"{accuracy:.1f}%",
                        f"{avg_time:.2f}s"
                    ))

        headers = ['Nick', 'Zamek', 'Skuteczność', 'Średni czas']
        columns = list(zip(*([headers] + rows))) if rows else [headers]
        col_widths = [max(len(str(item)) for item in col) for col in columns]

        def center(text, width):
            text = str(text)
            space = width - len(text)
            left = space // 2
            right = space - left
            return ' ' * left + text + ' ' * right

        sep_line = '|' + '|'.join(['-' * w for w in col_widths]) + '|'
        header_line = '|' + '|'.join(center(h, w) for h, w in zip(headers, col_widths)) + '|'
        row_lines = ['|' + '|'.join(center(c, w) for c, w in zip(row, col_widths)) + '|' for row in rows]

        return '\n'.join([header_line, sep_line] + row_lines)

def send_to_discord(*table_texts):
    for table_text in table_texts:
        data = {
            "content": "```\n" + table_text + "\n```"
        }
        try:
            r = requests.post(DISCORD_WEBHOOK_URL, json=data, timeout=10)
            if r.status_code != 204:
                print(f"[ERROR] Wysyłanie do Discorda nie powiodło się, status: {r.status_code}, treść: {r.text}")
        except Exception as e:
            print(f"[ERROR] Błąd podczas wysyłania do Discorda: {e}")

def initial_load_and_process():
    global last_log_filename, last_processed_line
    try:
        ftp = connect_ftp()
        logs = list_logs(ftp)
        if not logs:
            print("[INFO] Brak logów na FTP.")
            ftp.quit()
            return

        logs.sort()
        last_log_filename = logs[-1]

        print(f"[INFO] Pobieram i przetwarzam wszystkie logi z FTP ({len(logs)} plików)...")
        for log in logs:
            fetch_and_parse_log(ftp, log)
        ftp.quit()
        last_processed_line = 0

        print("[INFO] Przetworzono wszystkie dostępne logi.")
        table = generate_table()
        short_table = generate_short_table()
        print(table)
        print(short_table)
        send_to_discord(table, short_table)

    except Exception as e:
        print(f"[ERROR] Błąd podczas pobierania listy lub przetwarzania: {e}")

def monitor_new_lines_loop():
    global last_log_filename, last_processed_line
    while True:
        time.sleep(60)
        try:
            ftp = connect_ftp()
            logs = list_logs(ftp)
            if not logs:
                ftp.quit()
                continue

            logs.sort()
            current_log = logs[-1]

            if current_log != last_log_filename:
                last_log_filename = current_log
                last_processed_line = 0
                print(f"[INFO] Zmiana loga na {current_log}, resetuję pozycję czytania.")

            processed, total_lines = fetch_and_parse_log_incremental(ftp, last_log_filename, last_processed_line)
            if processed > 0:
                last_processed_line = total_lines
                table = generate_table()
                short_table = generate_short_table()
                print(table)
                print(short_table)
                send_to_discord(table, short_table)
            ftp.quit()
        except Exception as e:
            print(f"[ERROR] Błąd podczas monitoringu nowych linii: {e}")

@app.route('/')
def index():
    return Response("Lockpicking service is running.", mimetype='text/plain')

if __name__ == '__main__':
    print("🔁 Uruchamianie pełnego przetwarzania logów...")
    initial_load_and_process()
    print("🔁 Start wątku do monitorowania nowych linii w najnowszym pliku...")
    t = threading.Thread(target=monitor_new_lines_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=10000)
