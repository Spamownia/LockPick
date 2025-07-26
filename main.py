import ftplib
import psycopg2
import re
import io
import requests
from collections import defaultdict
from datetime import datetime

# --- Konfiguracja FTP i Webhook ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# --- Konfiguracja bazy danych PostgreSQL (Neon) ---
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- Połączenie z bazą danych ---
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking (
            nick TEXT,
            castle TEXT,
            result TEXT,
            time FLOAT,
            UNIQUE(nick, castle, result, time)
        );
    """)
    conn.commit()
    return conn, cur

# --- Pobranie plików z FTP ---
def download_logs():
    print("[INFO] Pobieranie logów...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_LOG_DIR)
    all_files = ftp.nlst()
    filenames = [f for f in all_files if f.startswith("gameplay_") and f.endswith(".log")]

    logs = []
    for filename in filenames:
        with io.BytesIO() as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
            content = f.getvalue().decode("utf-16-le", errors="ignore")
            logs.append(content)
    ftp.quit()
    return logs

# --- Parsowanie danych z logów ---
def parse_logs(logs):
    print("[INFO] Parsowanie danych...")
    data = []
    pattern = re.compile(
        r"(?P<nick>\w+)\s+tried to pick the (?P<castle>\w+) lock and (?P<result>succeeded|failed)(?: in (?P<time>\d+(?:[.,]\d+)?)s)?"
    )
    for content in logs:
        for match in pattern.finditer(content):
            nick = match.group("nick")
            castle = match.group("castle")
            result = match.group("result")
            time = match.group("time")
            time_val = float(time.replace(",", ".")) if time else None
            data.append((nick, castle, result, time_val))
    return data

# --- Zapis do bazy danych ---
def store_data(cur, conn, data):
    print(f"[INFO] Zapis do bazy: {len(data)} rekordów")
    for nick, castle, result, time_val in data:
        try:
            cur.execute("""
                INSERT INTO lockpicking (nick, castle, result, time)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (nick, castle, result, time_val))
        except Exception as e:
            print(f"[BŁĄD] {e}")
    conn.commit()

# --- Agregacja danych i przygotowanie tabeli ---
def build_table(cur):
    print("[INFO] Generowanie tabeli...")
    cur.execute("SELECT nick, castle, result, time FROM lockpicking;")
    rows = cur.fetchall()

    summary = defaultdict(lambda: {"total": 0, "success": 0, "fail": 0, "times": []})

    for nick, castle, result, time in rows:
        key = (nick, castle)
        summary[key]["total"] += 1
        if result == "succeeded":
            summary[key]["success"] += 1
            if time is not None:
                summary[key]["times"].append(time)
        else:
            summary[key]["fail"] += 1

    table = []
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    rows_text = []

    for (nick, castle), stats in summary.items():
        total = stats["total"]
        success = stats["success"]
        fail = stats["fail"]
        effectiveness = f"{(success / total * 100):.1f}%" if total else "0%"
        avg_time = f"{(sum(stats['times']) / len(stats['times'])):.2f}s" if stats["times"] else "-"
        row = [nick, castle, str(total), str(success), str(fail), effectiveness, avg_time]
        rows_text.append(row)

    # Wyznacz szerokości kolumn
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*([headers] + rows_text))]

    def format_row(row):
        return "| " + " | ".join(f"{cell:^{col_widths[i]}}" for i, cell in enumerate(row)) + " |"

    separator = "|-" + "-|-".join("-" * w for w in col_widths) + "-|"
    formatted_table = [format_row(headers), separator] + [format_row(row) for row in rows_text]

    return "```\n" + "\n".join(formatted_table) + "\n```"

# --- Wysyłanie tabeli na webhook Discord ---
def send_to_webhook(table):
    print("[INFO] Wysyłanie na webhook...")
    response = requests.post(WEBHOOK_URL, json={"content": table})
    if response.status_code != 204:
        print(f"[BŁĄD] Webhook zwrócił {response.status_code}: {response.text}")

# --- Główna funkcja ---
def main():
    print("[INFO] Inicjalizacja bazy...")
    conn, cur = init_db()

    logs = download_logs()
    if not logs:
        print("[INFO] Brak logów do przetworzenia.")
        return

    parsed = parse_logs(logs)
    store_data(cur, conn, parsed)

    table = build_table(cur)
    send_to_webhook(table)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
