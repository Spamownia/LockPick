import ftplib
import io
import re
import psycopg2
import statistics
import requests
from collections import defaultdict

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

LOG_PATTERN = re.compile(
    r"User:\s*(?P<nick>.*?)\s*\|\s*Lock:\s*(?P<lock>.*?)\s*\|\s*Success:\s*(?P<success>Yes|No)\s*\|\s*Elapsed time:\s*(?P<time>[\d.]+)"
)

def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)
    print("[OK] Połączono z FTP:", FTP_HOST)
    return ftp

def list_log_files(ftp):
    files = []
    ftp.retrlines('LIST', lambda line: files.append(line.split()[-1]))
    return [f for f in files if f.startswith("gameplay_") and f.endswith(".log")]

def download_log_file(ftp, filename):
    buffer = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)
    content = buffer.read().decode("utf-16-le", errors="ignore")
    print(f"[INFO] Wczytano plik: {filename}")
    return content

def parse_log_content(content):
    matches = LOG_PATTERN.finditer(content)
    return [
        {
            "nick": m.group("nick").strip(),
            "lock": m.group("lock").strip(),
            "success": m.group("success") == "Yes",
            "time": float(m.group("time"))
        }
        for m in matches
    ]

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_stats (
            nick TEXT,
            lock TEXT,
            success BOOLEAN,
            time FLOAT
        )
    """)
    conn.commit()
    return conn

def insert_entries(conn, entries):
    cursor = conn.cursor()
    for entry in entries:
        cursor.execute("""
            INSERT INTO lockpick_stats (nick, lock, success, time)
            VALUES (%s, %s, %s, %s)
        """, (entry["nick"], entry["lock"], entry["success"], entry["time"]))
    conn.commit()

def generate_summary(entries):
    stats = defaultdict(lambda: defaultdict(list))
    for e in entries:
        stats[(e["nick"], e["lock"])]["results"].append(e["success"])
        stats[(e["nick"], e["lock"])]["times"].append(e["time"])

    rows = []
    for (nick, lock), data in stats.items():
        results = data["results"]
        times = data["times"]
        total = len(results)
        success = sum(results)
        fail = total - success
        accuracy = f"{(success / total * 100):.1f}%"
        avg_time = f"{statistics.mean(times):.2f}s"
        rows.append([nick, lock, str(total), str(success), str(fail), accuracy, avg_time])

    return rows

def format_table(rows):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = [headers] + rows
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*table)]

    def fmt_row(row):
        return " | ".join(str(cell).center(width) for cell, width in zip(row, col_widths))

    formatted = "```\n" + "\n".join(fmt_row(row) for row in table) + "\n```"
    return formatted

def send_to_discord(table_text):
    data = {"content": table_text}
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code == 204:
        print("[OK] Tabela wysłana na Discord.")
    else:
        print("[BŁĄD] Nie udało się wysłać na Discord:", response.text)

def fetch_and_parse_logs():
    ftp = connect_ftp()
    files = list_log_files(ftp)
    all_entries = []
    for filename in files:
        content = download_log_file(ftp, filename)
        entries = parse_log_content(content)
        all_entries.extend(entries)
    ftp.quit()
    return all_entries

def main():
    print("[DEBUG] Start programu")
    entries = fetch_and_parse_logs()
    if not entries:
        print("[INFO] Brak nowych danych do przetworzenia.")
        return

    conn = init_db()
    insert_entries(conn, entries)
    summary = generate_summary(entries)
    if summary:
        table_text = format_table(summary)
        print(table_text)
        send_to_discord(table_text)
    conn.close()

if __name__ == "__main__":
    main()
