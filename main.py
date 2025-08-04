import os
import re
import ftplib
import time
import threading
from datetime import datetime
from flask import Flask
import pytz
import requests
from io import BytesIO
import codecs

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

LOG_FILE_REGEX = re.compile(r"gameplay_.*\.log")

stats = {}
processed_entries = set()

def format_table():
    headers = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni_czas"]
    rows = []

    for nick in sorted(stats):
        for lock in sorted(stats[nick]):
            s = stats[nick][lock]
            total = s['success'] + s['fail']
            skutecznosc = (s['success'] / total * 100) if total > 0 else 0
            avg_time = (s['total_time'] / total) if total > 0 else 0
            rows.append([
                nick,
                lock,
                str(total),
                str(s['success']),
                str(s['fail']),
                f"{skutecznosc:.1f}%",
                f"{avg_time:.2f}s"
            ])

    col_widths = [max(len(str(x)) for x in col) for col in zip(*([headers] + rows))]
    line = "| " + " | ".join(f"{headers[i].center(col_widths[i])}" for i in range(len(headers))) + " |"
    sep = "|" + "|".join("-" * (col_widths[i] + 2) for i in range(len(headers))) + "|"
    lines = [line, sep]
    for row in rows:
        lines.append("| " + " | ".join(f"{row[i].center(col_widths[i])}" for i in range(len(row))) + " |")

    return "```\n" + "\n".join(lines) + "\n```"

def send_to_discord(content):
    requests.post(DISCORD_WEBHOOK, json={"content": content})

def parse_line(line):
    pattern = r'\[(.*?)\].*?CharacterName: (.*?), Lock type: (.*?), Success: (.*?), Time: (.*?)s'
    match = re.search(pattern, line)
    if not match:
        return None
    timestamp, nick, lock_type, success, time_taken = match.groups()
    uid = f"{timestamp}-{nick}-{lock_type}-{success}-{time_taken}"
    if uid in processed_entries:
        return None
    processed_entries.add(uid)
    return nick, lock_type, success.lower() == 'true', float(time_taken)

def update_stats(nick, lock, success, time_taken):
    if nick not in stats:
        stats[nick] = {}
    if lock not in stats[nick]:
        stats[nick][lock] = {'success': 0, 'fail': 0, 'total_time': 0.0}
    entry = stats[nick][lock]
    if success:
        entry['success'] += 1
    else:
        entry['fail'] += 1
    entry['total_time'] += time_taken

def fetch_logs_from_ftp():
    logs = []
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_PATH)
        files = []
        ftp.retrlines('LIST', lambda x: files.append(x.split()[-1]))
        for filename in files:
            if LOG_FILE_REGEX.match(filename):
                bio = BytesIO()
                ftp.retrbinary(f"RETR {filename}", bio.write)
                bio.seek(0)
                content = bio.read()
                try:
                    text = codecs.decode(content, 'utf-16-le')
                    logs.append(text)
                except UnicodeDecodeError:
                    continue
    return logs

def process_logs(log_texts):
    new_data = False
    for text in log_texts:
        for line in text.splitlines():
            result = parse_line(line)
            if result:
                update_stats(*result)
                new_data = True
    if new_data:
        send_to_discord(format_table())

def monitor():
    last_snapshot = ""
    while True:
        try:
            logs = fetch_logs_from_ftp()
            if logs:
                latest = logs[-1]
                if latest != last_snapshot:
                    process_logs([latest])
                    last_snapshot = latest
        except Exception as e:
            print(f"❌ Błąd w monitorowaniu: {e}")
        time.sleep(60)

def start():
    print("🔁 Uruchamianie skanowania logów...")
    try:
        logs = fetch_logs_from_ftp()
        process_logs(logs)
    except Exception as e:
        print(f"❌ Błąd inicjalizacji: {e}")

    threading.Thread(target=monitor, daemon=True).start()

app = Flask(__name__)

@app.route("/")
def index():
    return "Lockpick stats logger active."

if __name__ == "__main__":
    start()
    app.run(host="0.0.0.0", port=10000)
