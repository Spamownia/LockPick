import os
import re
import time
import ftplib
import threading
from io import BytesIO
from datetime import datetime
from collections import defaultdict

import requests
from flask import Flask
from PIL import Image, ImageDraw, ImageFont

# Automatyczna instalacja Pillow, jeśli nie jest zainstalowany
try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    import subprocess
    subprocess.check_call(["pip", "install", "Pillow"])
    from PIL import Image, ImageDraw, ImageFont

FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DISCORD_WEBHOOK_FULL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
DISCORD_WEBHOOK_SHORT = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
DISCORD_WEBHOOK_PODIUM = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

CHECK_INTERVAL = 60
LOG_ENCODING = "utf-8"

castle_order = ["VeryEasy", "Basic", "Medium", "Advanced", "DialLock"]
stats = defaultdict(lambda: defaultdict(lambda: {"success": 0, "fail": 0, "all": 0, "times": []}))
last_positions = defaultdict(int)

def parse_log_line(line):
    match = re.search(r'\[(.*?)\].*?(.+?) tried to pick (\w+) lock - Success: (\w+) \(([\d.]+)\)', line)
    if match:
        timestamp = match.group(1)
        nick = match.group(2)
        lock_type = match.group(3)
        success = match.group(4) == "True"
        elapsed = float(match.group(5).replace(",", ".").replace(" ", "").replace("..", ".").replace("’", "").strip("."))
        return {"timestamp": timestamp, "nick": nick, "lock": lock_type, "success": success, "elapsed": elapsed}
    return None

def process_line(line):
    parsed = parse_log_line(line)
    if parsed:
        user = parsed["nick"]
        lock = parsed["lock"]
        success = parsed["success"]
        elapsed = parsed["elapsed"]

        s = stats[user][lock]
        if success:
            s["success"] += 1
            s["fail"] += s["fail"]
            s["all"] += 1 + s["fail"]
        else:
            s["fail"] += 1
            s["all"] += 1
        s["times"].append(elapsed)

def fetch_logs():
    logs = []
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_DIR)
            filenames = []

            ftp.retrlines('LIST', lambda x: filenames.append(x.split()[-1]))
            for filename in filenames:
                if not filename.endswith(".log"):
                    continue
                r = []
                ftp.retrlines(f"RETR {filename}", r.append)
                logs.append((filename, r))
    except Exception as e:
        print(f"Błąd FTP: {e}")
    return logs

def generate_table_image(headers, rows, title="Statystyki"):
    font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
    font = ImageFont.truetype(font_path, 16)

    padding_x = 20
    padding_y = 10
    cell_margin = 5

    column_widths = [max(len(str(cell)) for cell in col) * 10 + padding_x for col in zip(headers, *rows)]

    table_width = sum(column_widths) + cell_margin * (len(headers) + 1)
    table_height = (len(rows) + 1) * (font.size + padding_y) + 40

    image = Image.new("RGB", (table_width, table_height), "white")
    draw = ImageDraw.Draw(image)

    y = 20
    x = cell_margin
    for i, header in enumerate(headers):
        draw.text((x, y), header, fill="black", font=font)
        x += column_widths[i] + cell_margin

    y += font.size + padding_y
    for row in rows:
        x = cell_margin
        for i, cell in enumerate(row):
            draw.text((x, y), str(cell), fill="black", font=font)
            x += column_widths[i] + cell_margin
        y += font.size + padding_y

    output = BytesIO()
    image.save(output, format="PNG")
    output.seek(0)
    return output

def send_to_discord(image_data, webhook_url, filename="tabela.png"):
    try:
        files = {'file': (filename, image_data, 'image/png')}
        response = requests.post(webhook_url, files=files)
        if response.status_code >= 400:
            print(f"❌ Błąd wysyłania obrazu do Discord: {response.status_code}")
    except Exception as e:
        print(f"❌ Wyjątek przy wysyłce obrazu do Discord: {e}")

def generate_full_table_image():
    headers = ["Nick"] + [f"{lock}" for lock in castle_order]
    rows = []
    for nick, locks in stats.items():
        row = [nick]
        for lock in castle_order:
            s = locks[lock]
            row.append(f"{s['success']}/{s['all']}")
        rows.append(row)
    return generate_table_image(headers, rows, title="Pełna tabela")

def generate_short_table_image():
    headers = ["Nick", "Zamek", "Skuteczność", "Średni czas"]
    rows = []
    for nick, locks in stats.items():
        for lock in castle_order:
            s = locks[lock]
            if s["all"] == 0:
                continue
            success_rate = round(100 * s["success"] / s["all"], 2)
            avg_time = round(sum(s["times"]) / len(s["times"]), 2) if s["times"] else 0
            rows.append([nick, lock, f"{success_rate}%", f"{avg_time}s"])
    return generate_table_image(headers, rows, title="Skrócona tabela")

def generate_podium_table_image():
    headers = ["🏆", "Nick", "Skuteczność"]
    user_rates = []
    for nick, locks in stats.items():
        total_success = sum(s["success"] for s in locks.values())
        total_all = sum(s["all"] for s in locks.values())
        if total_all == 0:
            continue
        success_rate = round(100 * total_success / total_all, 2)
        user_rates.append((nick, success_rate))
    user_rates.sort(key=lambda x: x[1], reverse=True)

    medals = ["🥇", "🥈", "🥉"]
    rows = []
    for idx, (nick, rate) in enumerate(user_rates):
        place = medals[idx] if idx < 3 else str(idx + 1)
        rows.append([place, nick, f"{rate}%"])
    return generate_table_image(headers, rows[:10], title="Podium")

def process_all_logs():
    print("🔁 Uruchamianie pełnego przetwarzania logów...")
    logs = fetch_logs()
    for _, lines in logs:
        for line in lines:
            process_line(line)
    send_all_tables()

def send_all_tables():
    send_to_discord(generate_full_table_image(), DISCORD_WEBHOOK_FULL, "pelna.png")
    send_to_discord(generate_short_table_image(), DISCORD_WEBHOOK_SHORT, "skrocona.png")
    send_to_discord(generate_podium_table_image(), DISCORD_WEBHOOK_PODIUM, "podium.png")

def monitor_new_entries():
    last_seen = {}
    while True:
        try:
            logs = fetch_logs()
            for filename, lines in logs:
                pos = last_seen.get(filename, 0)
                new_lines = lines[pos:]
                for line in new_lines:
                    process_line(line)
                if new_lines:
                    last_seen[filename] = len(lines)
                    send_all_tables()
        except Exception as e:
            print(f"⛔ Błąd podczas monitorowania logów: {e}")
        time.sleep(CHECK_INTERVAL)

app = Flask(__name__)

@app.route("/")
def index():
    return "SCUM Lockpicking Stats Service"

if __name__ == "__main__":
    threading.Thread(target=monitor_new_entries, daemon=True).start()
    process_all_logs()
    app.run(host="0.0.0.0", port=10000)
