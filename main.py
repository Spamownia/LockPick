import os
import re
import time
import threading
import ftplib
from collections import defaultdict
from flask import Flask
import requests

FTP_HOST = "ftp.host"
FTP_USER = "user"
FTP_PASS = "pass"
FTP_DIR = "/Scum/Logs"

DISCORD_WEBHOOK_FULL = "https://discord.com/api/webhooks/1396227086527762632/HDWBcc5rVBDbimFdh-fuE43iL8inA6YXpLuYG2a4cUmbF8RQyLqtohx-1pWaQMzBzXlf"
DISCORD_WEBHOOK_SHORT = "https://discord.com/api/webhooks/1403070347280126132/hcMfNpXKmnnHhdylhvqvqVMnRkqzdztLf0lSQ_Lo9gs2joaqUaU0KQGBmSN8Qp88ZYaH"
DISCORD_WEBHOOK_PODIUM = "https://discord.com/api/webhooks/1396229119456448573/PG0jkv4VBlihDwkibrn3jGZ0k516O47iTWb1dziuvoGVKVoqffLqm8GmPLbVHvpJtYhv"

app = Flask(__name__)

log_pattern = re.compile(
    r'\[(.*?)\].*?Player (.*?) tried to pick (.*?) lock: Success: (\w+), Time: ([\d.]+)s, Failed attempts: (\d+)'
)

lock_order = ['VeryEasy', 'Basic', 'Medium', 'Advanced', 'DialLock']
stats = defaultdict(lambda: defaultdict(lambda: {'all': 0, 'success': 0, 'fail': 0, 'time': 0.0}))
stats_lock = threading.Lock()
processed_lines = set()
last_log_name = None
last_line_count = 0

def parse_log_line(line):
    match = log_pattern.search(line)
    if match:
        _, player, lock, success, duration, fails = match.groups()
        player = player.strip()
        lock = lock.strip()
        success = success.lower() == 'yes'
        duration = float(duration)
        fails = int(fails)

        if success:
            all_attempts = 1 + fails
            success_count = 1
            fail_count = fails
        else:
            all_attempts = fails
            success_count = 0
            fail_count = fails

        return player, lock, all_attempts, success_count, fail_count, duration
    return None

def fetch_logs():
    with ftplib.FTP(FTP_HOST) as ftp:
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        files = ftp.nlst()
        logs = {}

        for filename in files:
            if filename.endswith(".log"):
                with open(filename, 'wb') as f:
                    ftp.retrbinary(f"RETR {filename}", f.write)
                with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                    logs[filename] = f.readlines()

        return logs

def update_stats(lines):
    global processed_lines
    with stats_lock:
        for line in lines:
            if line in processed_lines:
                continue
            parsed = parse_log_line(line)
            if parsed:
                player, lock, all_, success_, fail_, time_ = parsed
                data = stats[player][lock]
                data['all'] += all_
                data['success'] += success_
                data['fail'] += fail_
                data['time'] += time_
            processed_lines.add(line)

def generate_tables():
    full_rows = []
    short_rows = []

    with stats_lock:
        for nick in sorted(stats.keys()):
            for lock in lock_order:
                data = stats[nick].get(lock)
                if not data or data['all'] == 0:
                    continue
                skutecznosc = data['success'] / data['all'] * 100
                sr_czas = data['time'] / data['all']
                full_rows.append([
                    nick, lock,
                    str(data['all']),
                    str(data['success']),
                    str(data['fail']),
                    f"{skutecznosc:.1f}%",
                    f"{sr_czas:.2f}s"
                ])
                short_rows.append([
                    nick, lock,
                    f"{skutecznosc:.1f}%",
                    f"{sr_czas:.2f}s"
                ])

    full_table = format_table(
        ['Nick', 'Zamek', 'Wszystkie', 'Udane', 'Nieudane', 'Skuteczność', 'Średni czas'],
        full_rows
    )
    short_table = format_table(
        ['Nick', 'Zamek', 'Skuteczność', 'Średni czas'],
        short_rows
    )
    podium_table = generate_podium_table()

    return full_table, short_table, podium_table

def format_table(headers, rows):
    columns = list(zip(*([headers] + rows))) if rows else [headers]
    col_widths = [max(len(str(item)) for item in col) + 4 for col in columns]

    def center(text, width):
        text = str(text)
        space = width - len(text)
        return ' ' * (space // 2) + text + ' ' * (space - space // 2)

    sep_line = '|' + '|'.join(['-' * w for w in col_widths]) + '|'
    header_line = '|' + '|'.join(center(h, w) for h, w in zip(headers, col_widths)) + '|'
    row_lines = ['|' + '|'.join(center(c, w) for c, w in zip(row, col_widths)) + '|' for row in rows]

    return '\n'.join([header_line, sep_line] + row_lines)

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
            rows.append((place, nick, f"{acc:.1f}%"))

        return format_table(['Miejsce', 'Nick', 'Skuteczność'], rows)

def send_discord_message(webhook_url, content):
    requests.post(webhook_url, json={"content": f"```\n{content}\n```"})

def background_worker():
    global last_log_name, last_line_count
    logs = fetch_logs()
    first_run = True

    if not logs:
        print("Brak logów do przetworzenia.")
        return

    for logname in sorted(logs.keys()):
        update_stats(logs[logname])
        last_log_name = logname
        last_line_count = len(logs[logname])

    full, short, podium = generate_tables()
    send_discord_message(DISCORD_WEBHOOK_FULL, full)
    send_discord_message(DISCORD_WEBHOOK_SHORT, short)
    send_discord_message(DISCORD_WEBHOOK_PODIUM, podium)
    print(full)
    print(short)
    print(podium)

    while True:
        time.sleep(60)
        try:
            logs = fetch_logs()
            if last_log_name not in logs:
                continue

            lines = logs[last_log_name]
            new_lines = lines[last_line_count:]
            if new_lines:
                update_stats(new_lines)
                last_line_count = len(lines)

                full, short, podium = generate_tables()
                send_discord_message(DISCORD_WEBHOOK_FULL, full)
                send_discord_message(DISCORD_WEBHOOK_SHORT, short)
                send_discord_message(DISCORD_WEBHOOK_PODIUM, podium)
                print(full)
                print(short)
                print(podium)

        except Exception as e:
            print(f"Błąd w pętli: {e}")

@app.route('/')
def index():
    return "SCUM Log Parser Active"

if __name__ == '__main__':
    threading.Thread(target=background_worker, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
