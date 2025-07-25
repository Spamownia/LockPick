import ftplib
import time
import re
import io
import requests
import pandas as pd
from flask import Flask

# --- KONFIGURACJA FTP I WEBHOOK ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- REGEX DO WYCIĄGANIA LOCKPICKÓW ---
LOCKPICK_REGEX = re.compile(
    r"User:\s+(.*?)\s+\(\d+,\s+\d+\).*?Success:\s+(Yes|No).*?Elapsed time:\s+([\d.,]+).*?Lock type:\s+(\w+)",
    re.DOTALL
)

app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# --- POBIERANIE LOGÓW Z FTP ---
def fetch_logs():
    logs = []
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.encoding = 'utf-8'
            ftp.cwd(LOG_DIR)
            filenames = ftp.nlst()

            for filename in filenames:
                if filename.startswith("gameplay_") and filename.endswith(".log"):
                    r = io.BytesIO()
                    ftp.retrbinary(f"RETR {filename}", r.write)
                    data = r.getvalue().decode('utf-16le')
                    logs.append(data)
    except Exception as e:
        print(f"[ERROR] FTP download: {e}")
    return logs

# --- PARSOWANIE LOGÓW LOCKPICK ---
def parse_lockpicks(log_texts):
    stats = {}
    for text in log_texts:
        matches = LOCKPICK_REGEX.findall(text)
        for nick, success, elapsed, locktype in matches:
            key = (nick, locktype)
            if key not in stats:
                stats[key] = {"total": 0, "success": 0, "fail": 0, "times": []}

            stats[key]["total"] += 1
            if success.lower() == "yes":
                stats[key]["success"] += 1
            else:
                stats[key]["fail"] += 1

            clean_elapsed = elapsed.strip().replace(",", ".")
            clean_elapsed = re.sub(r"[^\d.]", "", clean_elapsed)
            if clean_elapsed:
                stats[key]["times"].append(float(clean_elapsed))
    return stats

# --- TWORZENIE TABELI EXCEL ---
def build_table(stats):
    rows = []
    for (nick, locktype), data in stats.items():
        total = data["total"]
        success = data["success"]
        fail = data["fail"]
        avg_time = sum(data["times"]) / len(data["times"]) if data["times"] else 0
        effectiveness = f"{(success/total)*100:.1f}%" if total else "0%"
        rows.append([nick, locktype, total, success, fail, effectiveness, f"{avg_time:.2f}s"])

    df = pd.DataFrame(rows, columns=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"])

    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Lockpicks')
        workbook = writer.book
        worksheet = writer.sheets['Lockpicks']

        for i, col in enumerate(df.columns):
            max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, max_length, workbook.add_format({'align': 'center'}))

        header_format = workbook.add_format({'bold': True, 'align': 'center'})
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

    output.seek(0)
    return output

# --- WYSYŁANIE NA DISCORD WEBHOOK ---
def send_to_discord(file):
    files = {"file": ("lockpicks.xlsx", file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    response = requests.post(WEBHOOK_URL, files=files)
    print(f"[INFO] Webhook status: {response.status_code}")

# --- PĘTLA GŁÓWNA ---
if __name__ == "__main__":
    while True:
        print("[INFO] Sprawdzanie logów FTP...")
        logs = fetch_logs()
        if logs:
            stats = parse_lockpicks(logs)
            excel_file = build_table(stats)
            send_to_discord(excel_file)
        else:
            print("[INFO] Brak nowych logów.")
        time.sleep(60)

    app.run(host='0.0.0.0', port=3000)
