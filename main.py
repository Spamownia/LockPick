import os
import io
import re
import time
import pandas as pd
import psycopg2
import requests
from ftplib import FTP
from tabulate import tabulate
from flask import Flask

# === KONFIGURACJA ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

LOCK_ORDER = {'VeryEasy': 0, 'Basic': 1, 'Medium': 2, 'Advanced': 3, 'DialLock': 4}

app = Flask(__name__)


def get_latest_log_filename(ftp):
    filenames = []
    ftp.retrlines(f"LIST {FTP_LOG_DIR}", lambda line: filenames.append(line.split()[-1]))
    gameplay_logs = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
    if not gameplay_logs:
        return None
    return sorted(gameplay_logs)[-1]


def download_log_file(ftp, filename):
    log_data = io.BytesIO()
    ftp.retrbinary(f"RETR {FTP_LOG_DIR}{filename}", log_data.write)
    return log_data.getvalue().decode("utf-16-le")


def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<Nick>.+?) "
        r"\(\d+, \d+\)\. Success: (?P<Success>Yes|No)\. Elapsed time: (?P<Time>[0-9.]+)\. .*? Lock type: (?P<LockType>\w+)\."
    )
    data = []
    for match in pattern.finditer(content):
        data.append({
            "Nick": match.group("Nick").strip(),
            "Success": match.group("Success").strip(),
            "Time": float(match.group("Time")),
            "LockType": match.group("LockType").strip()
        })
    print(f"[DEBUG] Rozpoznano wpisów: {len(data)}")
    return data


def create_dataframe(data):
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df["Total"] = 1
    df["SuccessCount"] = df["Success"].apply(lambda x: 1 if x == "Yes" else 0)
    df["FailCount"] = df["Success"].apply(lambda x: 0 if x == "Yes" else 1)

    summary = df.groupby(["Nick", "LockType"], sort=False).agg(
        Attempts=("Total", "sum"),
        Successes=("SuccessCount", "sum"),
        Fails=("FailCount", "sum"),
        AvgTime=("Time", "mean")
    ).reset_index()

    summary["SuccessRate"] = (summary["Successes"] / summary["Attempts"] * 100).round(1).astype(str) + "%"
    summary["AvgTime"] = summary["AvgTime"].round(2)

    summary = summary.sort_values(
        by=["Nick", "LockType"],
        key=lambda col: col.map(LOCK_ORDER) if col.name == "LockType" else col
    )

    return summary[["Nick", "LockType", "Attempts", "Successes", "Fails", "SuccessRate", "AvgTime"]]


def send_to_discord(df):
    if df.empty:
        print("[INFO] Brak danych do wysłania.")
        return

    headers = {
        "Nick": "Nick",
        "LockType": "Zamek",
        "Attempts": "Ilość wszystkich prób",
        "Successes": "Udane",
        "Fails": "Nieudane",
        "SuccessRate": "Skuteczność",
        "AvgTime": "Średni czas"
    }

    df_renamed = df.rename(columns=headers)
    table = tabulate(df_renamed, headers="keys", tablefmt="grid", stralign="center", numalign="center")

    payload = {
        "content": f"```\n{table}\n```"
    }

    response = requests.post(WEBHOOK_URL, json=payload)
    print(f"[INFO] Wysłano dane do Discorda: {response.status_code}")


def main_loop():
    last_processed = ""
    while True:
        try:
            print("[DEBUG] Łączenie z FTP...")
            with FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT)
                ftp.login(FTP_USER, FTP_PASS)

                filename = get_latest_log_filename(ftp)
                if not filename:
                    print("[WARN] Brak plików logów.")
                    time.sleep(60)
                    continue

                if filename == last_processed:
                    print("[INFO] Brak nowych logów.")
                    time.sleep(60)
                    continue

                print(f"[DEBUG] Przetwarzanie pliku: {filename}")
                content = download_log_file(ftp, filename)
                data = parse_log_content(content)
                df = create_dataframe(data)
                send_to_discord(df)
                last_processed = filename

        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(60)


@app.route("/")
def index():
    return "Alive"


if __name__ == "__main__":
    print("[DEBUG] Start main_loop")
    main_loop()
