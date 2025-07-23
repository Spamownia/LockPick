import subprocess
import sys

def silent_install(package):
    try:
        __import__(package)
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", package],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

silent_install("requests")
silent_install("flask")

import re
import csv
import statistics
import requests
import os
import time
import threading
from collections import defaultdict
from ftplib import FTP
from io import BytesIO
from flask import Flask

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def send_discord(content, webhook_url):
    requests.post(webhook_url, json={"content": content})

FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

pattern = re.compile(
    r"User: (?P<nick>[\w\d]+) \(\d+, [\d]+\)\. "
    r"Success: (?P<success>Yes|No)\. "
    r"Elapsed time: (?P<elapsed>[\d\.]+)\. "
    r"Failed attempts: (?P<failed_attempts>\d+)\. "
    r"Target object: [^\)]+\)\. "
    r"Lock type: (?P<lock_type>\w+)\."
)

def center(text, width):
    return text.center(width)

def process_loop():
    seen_lines = set()

    while True:
        ftp = FTP()
        ftp.connect(FTP_IP, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)

        log_files = []
        ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
        log_files = sorted([f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")])

        if not log_files:
            ftp.quit()
            time.sleep(60)
            continue

        latest_log = log_files[-1]

        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {latest_log}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

        ftp.quit()

        new_lines = [line for line in log_text.splitlines() if line not in seen_lines]
        if not new_lines:
            time.sleep(60)
            continue

        data_dict = {}
        if os.path.exists("logi.csv"):
            with open("logi.csv", "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) < 7: continue
                    nick, lock_type, all_attempts, succ, fail, eff, avg = row
                    key = (nick, lock_type)
                    data_dict[key] = {
                        "all_attempts": int(all_attempts),
                        "successful_attempts": int(succ),
                        "failed_attempts": int(fail),
                        "times": [float(avg.rstrip('s'))] * int(all_attempts)
                    }

        user_summary = defaultdict(lambda: {"success": 0, "total": 0, "times": []})

        for line in new_lines:
            match = pattern.search(line)
            if match:
                nick = match.group("nick")
                lock_type = match.group("lock_type")
                success = match.group("success")
                elapsed = float(match.group("elapsed"))

                key = (nick, lock_type)
                if key not in data_dict:
                    data_dict[key] = {
                        "all_attempts": 0,
                        "successful_attempts": 0,
                        "failed_attempts": 0,
                        "times": []
                    }

                data_dict[key]["all_attempts"] += 1
                if success == "Yes":
                    data_dict[key]["successful_attempts"] += 1
                else:
                    data_dict[key]["failed_attempts"] += 1
                data_dict[key]["times"].append(elapsed)

                user_summary[nick]["total"] += 1
                user_summary[nick]["times"].append(elapsed)
                if success == "Yes":
                    user_summary[nick]["success"] += 1

        with open("logi.csv", "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Nick", "Rodzaj zamka", "Ilość wszystkich prób", "Ilość udanych prób",
                             "Ilość nieudanych prób", "Skuteczność", "Śr. czas"])
            for (nick, lock_type), stats in data_dict.items():
                all_attempts = stats["all_attempts"]
                succ = stats["successful_attempts"]
                fail = stats["failed_attempts"]
                avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
                eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
                writer.writerow([nick, lock_type, all_attempts, succ, fail, f"{eff}%", f"{avg}s"])

        # Tabela główna
        headers = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skut.", "Śr. czas"]
        col_widths = [max(len(h), max((len(str(r[i])) for r in data_dict), default=0)) + 2 for i, h in enumerate(headers)]

        table_block = "```\n"
        table_block += "".join([center(h, len(h)+4) for h in headers]) + "\n"
        table_block += "-" * sum(col_widths) + "\n"

        for (nick, lock_type), stats in data_dict.items():
            all_attempts = stats["all_attempts"]
            succ = stats["successful_attempts"]
            fail = stats["failed_attempts"]
            avg = round(statistics.mean(stats["times"]), 2)
            eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
            row = [
                center(nick, len(headers[0])+4),
                center(lock_type, len(headers[1])+4),
                center(str(all_attempts), len(headers[2])+4),
                center(str(succ), len(headers[3])+4),
                center(str(fail), len(headers[4])+4),
                center(f"{eff}%", len(headers[5])+4),
                center(f"{avg}s", len(headers[6])+4)
            ]
            table_block += "".join(row) + "\n"

        table_block += "```"

        send_discord(table_block, WEBHOOK_URL)

        seen_lines.update(new_lines)
        time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=process_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
