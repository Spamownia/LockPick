import re
import os
import time
import requests
from collections import defaultdict
from datetime import datetime
from flask import Flask
from ftplib import FTP

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

def get_latest_log_from_ftp():
    with FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        files = ftp.nlst()
        log_files = [f for f in files if f.startswith("lockpick_") and f.endswith(".log")]
        if not log_files:
            print("[ERROR] Brak plików logów lockpick_*.log na FTP.")
            return None

        latest_file = max(log_files)
        local_path = os.path.join("/tmp", latest_file)

        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {latest_file}", f.write)

        return local_path

def parse_log(log_data):
    pattern = re.compile(
        r"User:\s+(?P<nick>\w+).*?Success:\s+(?P<success>\w+).*?Elapsed time:\s+(?P<time>[\d.]+).*?Failed attempts:\s+(?P<fails>\d+)",
        re.DOTALL
    )
    stats = defaultdict(lambda: {"success": 0, "fail": 0, "times": []})
    for match in pattern.finditer(log_data):
        nick = match.group("nick")
        success = match.group("success") == "Yes"
        time_taken = float(match.group("time"))
        if success:
            stats[nick]["success"] += 1
        else:
            stats[nick]["fail"] += 1
        stats[nick]["times"].append(time_taken)
    return stats

def generate_html_table(stats):
    rows = []
    for nick, data in sorted(stats.items(), key=lambda x: (x[1]["success"], -x[1]["fail"]), reverse=True):
        total = data["success"] + data["fail"]
        success_rate = f"{(data['success'] / total * 100):.1f}%" if total else "0%"
        avg_time = f"{(sum(data['times']) / len(data['times'])):.2f}" if data["times"] else "-"
        rows.append(f"""
        <tr>
            <td style="text-align: center;">{nick}</td>
            <td style="text-align: center;">{data['success']}</td>
            <td style="text-align: center;">{data['fail']}</td>
            <td style="text-align: center;">{success_rate}</td>
            <td style="text-align: center;">{avg_time}</td>
        </tr>""")
    return """
<table border="1" style="border-collapse: collapse; text-align: center;">
<thead>
<tr>
<th style="text-align: center;">Nick</th>
<th style="text-align: center;">Success</th>
<th style="text-align: center;">Fail</th>
<th style="text-align: center;">Success %</th>
<th style="text-align: center;">Avg. Time</th>
</tr>
</thead>
<tbody>
""" + "\n".join(rows) + "</tbody></table>"

def main_loop():
    last_log_hash = None
    while True:
        try:
            log_path = get_latest_log_from_ftp()
            if not log_path:
                time.sleep(60)
                continue

            with open(log_path, "r", encoding="utf-16-le") as f:
                content = f.read()

            current_hash = hash(content)
            if current_hash == last_log_hash:
                print("[INFO] Brak nowych zdarzeń w logu.")
            else:
                last_log_hash = current_hash
                stats = parse_log(content)
                html = generate_html_table(stats)
                response = requests.post(WEBHOOK_URL, json={"content": html})
                if response.status_code == 204:
                    print(f"[OK] Tabela wysłana: {datetime.now()}")
                else:
                    print(f"[ERROR] Błąd wysyłania do webhooka: {response.status_code}")

        except Exception as e:
            print(f"[EXCEPTION] {e}")

        time.sleep(60)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=3000)
