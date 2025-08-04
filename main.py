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

stats_lock = threading.Lock()
stats = defaultdict(lambda: defaultdict(lambda: {
    'all': 0,
    'success': 0,
    'fail': 0,
    'total_time': 0.0
}))

processed_entries = set()
last_processed_line = 0
last_log_filename = None
last_sent_stats_hash = None

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_PATH)
    return ftp

def list_logs(ftp):
    lines = []
    ftp.retrlines('LIST', lines.append)
    return [line.split(maxsplit=8)[-1] for line in lines if line.endswith('.log') and 'gameplay_' in line]

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

def line_hash(line):
    return hash(line)

def update_stats(nick, lock, success, elapsed, fail):
    with stats_lock:
        entry = stats[nick][lock]
        if success:
            entry['all'] += 1 + fail
            entry['success'] += 1
            entry['fail'] += fail
        else:
            entry['all'] += fail
            entry['fail'] += fail
        entry['total_time'] += elapsed

def fetch_and_parse_log(ftp, filename):
    global processed_entries
    try:
        bio = io.BytesIO()
        ftp.retrbinary(f'RETR {filename}', bio.write)
        bio.seek(0)
        content = bio.read().decode('utf-16le', errors='ignore')
    except Exception as e:
        print(f"[ERROR] {filename}: {e}")
        return 0

    lines = content.splitlines()
    processed = 0
    for line in lines:
        h = line_hash(line)
        if h in processed_entries:
            continue
        res = parse_line(line)
        if res:
            nick, lock, success, elapsed, fail = res
            update_stats(nick, lock, success, elapsed, fail)
            processed_entries.add(h)
            processed += 1
    return processed

def fetch_and_parse_log_incremental(ftp, filename, from_line):
    global processed_entries
    try:
        bio = io.BytesIO()
        ftp.retrbinary(f'RETR {filename}', bio.write)
        bio.seek(0)
        content = bio.read().decode('utf-16le', errors='ignore')
    except Exception as e:
        print(f"[ERROR] {filename}: {e}")
        return 0, from_line

    lines = content.splitlines()
    new_lines = lines[from_line:]
    processed = 0
    for line in new_lines:
        h = line_hash(line)
        if h in processed_entries:
            continue
        res = parse_line(line)
        if res:
            nick, lock, success, elapsed, fail = res
            update_stats(nick, lock, success, elapsed, fail)
            processed_entries.add(h)
            processed += 1
    return processed, len(lines)

def format_table(headers, rows):
    columns = list(zip(*([headers] + rows))) if rows else [headers]
    col_widths = [max(len(str(item)) for item in col) for col in columns]

    def center(text, width):
        text = str(text)
        space = width - len(text)
        return ' ' * (space // 2) + text + ' ' * (space - space // 2)

    sep_line = '|' + '|'.join(['-' * w for w in col_widths]) + '|'
    header_line = '|' + '|'.join(center(h, w) for h, w in zip(headers, col_widths)) + '|'
    row_lines = ['|' + '|'.join(center(c, w) for c, w in zip(row, col_widths)) + '|' for row in rows]

    return '\n'.join([header_line, sep_line] + row_lines)

def generate_table():
    with stats_lock:
        rows = []
        for nick in sorted(stats.keys()):
            for lock in LOCK_ORDER:
                if lock in stats[nick]:
                    e = stats[nick][lock]
                    all_ = e['all']
                    success = e['success']
                    fail = e['fail']
                    accuracy = (success / all_ * 100) if all_ > 0 else 0.0
                    avg_time = (e['total_time'] / all_) if all_ > 0 else 0.0
                    rows.append((nick, lock, all_, success, fail, f"{accuracy:.1f}%", f"{avg_time:.2f}s"))
        return format_table(
            ['Nick', 'Zamek', 'Wszystkie', 'Udane', 'Nieudane', 'Skuteczność', 'Średni czas'],
            rows
        )

def generate_short_table():
    with stats_lock:
        rows = []
        for nick in sorted(stats.keys()):
            for lock in LOCK_ORDER:
                if lock in stats[nick]:
                    e = stats[nick][lock]
                    all_ = e['all']
                    success = e['success']
                    accuracy = (success / all_ * 100) if all_ > 0 else 0.0
                    avg_time = (e['total_time'] / all_) if all_ > 0 else 0.0
                    rows.append((nick, lock, f"{accuracy:.1f}%", f"{avg_time:.2f}s"))
        return format_table(['Nick', 'Zamek', 'Skuteczność', 'Średni czas'], rows)

def generate_podium_table():
    with stats_lock:
        summary = defaultdict(lambda: [0, 0])  # nick: [success, all]
        for nick, locks in stats.items():
            for data in locks.values():
                summary[nick][0] += data['success']
                summary[nick][1] += data['all']

        ranked = [
            (nick, s, a, (s / a * 100) if a > 0 else 0.0)
            for nick, (s, a) in summary.items()
            if a > 0
        ]
        ranked.sort(key=lambda x: x[3], reverse=True)

        medals = ['🥇', '🥈', '🥉']
        rows = []
        for idx, (nick, success, all_, acc) in enumerate(ranked, start=1):
            place = medals[idx - 1] if idx <= 3 else f"{idx}."
            rows.append((place, nick, all_, success, f"{acc:.1f}%"))
        return format_table(['Miejsce', 'Nick', 'Wszystkie', 'Udane', 'Skuteczność'], rows)

def stats_hash():
    with stats_lock:
        items = []
        for nick in sorted(stats.keys()):
            for lock in LOCK_ORDER:
                if lock in stats[nick]:
                    e = stats[nick][lock]
                    items.append((nick, lock, e['all'], e['success'], e['fail'], round(e['total_time'], 2)))
        return hash(tuple(items))

def send_to_discord(*table_texts):
    for table_text in table_texts:
        data = {
            "content": "```\n" + table_text + "\n```"
        }
        try:
            r = requests.post(DISCORD_WEBHOOK_URL, json=data, timeout=10)
            if r.status_code != 204:
                print(f"[ERROR] Discord HTTP {r.status_code}: {r.text}")
        except Exception as e:
            print(f"[ERROR] Discord post failed: {e}")

def initial_load_and_process():
    global last_log_filename, last_processed_line, last_sent_stats_hash
    try:
        ftp = connect_ftp()
        logs = list_logs(ftp)
        if not logs:
            print("[INFO] Brak logów.")
            ftp.quit()
            return
        logs.sort()
        last_log_filename = logs[-1]
        for log in logs:
            fetch_and_parse_log(ftp, log)
        ftp.quit()
        last_processed_line = 0

        table = generate_table()
        short = generate_short_table()
        podium = generate_podium_table()
        print(table)
        print(short)
        print(podium)
        send_to_discord(table, short, podium)
        last_sent_stats_hash = stats_hash()

    except Exception as e:
        print(f"[ERROR] Init: {e}")

def monitor_new_lines_loop():
    global last_log_filename, last_processed_line, last_sent_stats_hash, processed_entries
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
                processed_entries.clear()
                print(f"[INFO] Nowy log: {current_log}")

            processed, total_lines = fetch_and_parse_log_incremental(ftp, current_log, last_processed_line)
            if processed > 0:
                last_processed_line = total_lines
                current_hash = stats_hash()
                if current_hash != last_sent_stats_hash:
                    table = generate_table()
                    short = generate_short_table()
                    podium = generate_podium_table()
                    print(table)
                    print(short)
                    print(podium)
                    send_to_discord(table, short, podium)
                    last_sent_stats_hash = current_hash
            ftp.quit()
        except Exception as e:
            print(f"[ERROR] Monitor: {e}")

@app.route('/')
def index():
    return Response("Lockpicking service is running.", mimetype='text/plain')

if __name__ == '__main__':
    print("🔁 Start – przetwarzanie logów...")
    initial_load_and_process()
    threading.Thread(target=monitor_new_lines_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
