import ftplib
import io
import re
import psycopg2
import requests
from collections import defaultdict

# Dane dostępowe do FTP
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"

# Dane dostępowe do PostgreSQL (Neon)
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# Parsowanie danych z logów
def parse_log_content(content):
    pattern = re.compile(
        r'User:\s+(.*?)\s+\(.*?\)\.\s+Success:\s+(Yes|No)\.\s+Elapsed time:\s+([\d.]+)\.\s+Failed attempts:\s+(\d+).*?Lock type:\s+(.*?)\.',
        re.MULTILINE
    )
    results = []

    for match in pattern.finditer(content):
        nick = match.group(1)
        success = match.group(2) == 'Yes'
        elapsed = float(match.group(3).rstrip('.'))
        fails = int(match.group(4))
        locktype = match.group(5)

        results.append({
            "nick": nick,
            "success": success,
            "elapsed": elapsed,
            "fails": fails,
            "locktype": locktype
        })
    return results

# Zapis danych do bazy PostgreSQL
def save_to_db(entries):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lockpicking_logs (
                    id SERIAL PRIMARY KEY,
                    nick TEXT,
                    locktype TEXT,
                    success BOOLEAN,
                    elapsed FLOAT,
                    fails INTEGER
                );
            """)
            for e in entries:
                cur.execute("""
                    INSERT INTO lockpicking_logs (nick, locktype, success, elapsed, fails)
                    VALUES (%s, %s, %s, %s, %s);
                """, (e["nick"], e["locktype"], e["success"], e["elapsed"], e["fails"]))
        conn.commit()

# Pobieranie i przetwarzanie logów z FTP
def fetch_and_parse_logs():
    print("[DEBUG] Start programu")
    all_entries = []
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")
        ftp.cwd("/SCUM/Saved/SaveFiles/Logs/")
        filenames = []
        ftp.retrlines('LIST', lambda line: filenames.append(line.split()[-1]))
        log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]
        print(f"[INFO] Znaleziono {len(log_files)} plików gameplay_*.log")

        for filename in log_files:
            print(f"[INFO] Przetwarzanie: {filename}")
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode("utf-16-le", errors="ignore")
            parsed = parse_log_content(content)
            print(f"[DEBUG] Rozpoznano {len(parsed)} wpisów")
            all_entries.extend(parsed)

    return all_entries

# Generowanie i wysyłka tabeli
def generate_table(entries):
    grouped = defaultdict(list)
    for e in entries:
        key = (e["nick"], e["locktype"])
        grouped[key].append(e)

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = [headers]

    for (nick, locktype), records in grouped.items():
        total = len(records)
        success = sum(1 for r in records if r["success"])
        fails = total - success
        avg_time = sum(r["elapsed"] for r in records if r["success"]) / success if success else 0
        acc = f"{(success / total * 100):.1f}%" if total else "0%"
        table.append([
            nick,
            locktype,
            str(total),
            str(success),
            str(fails),
            acc,
            f"{avg_time:.2f}s"
        ])

    # Wyśrodkowanie i formatowanie kolumn
    col_widths = [max(len(row[i]) for row in table) for i in range(len(headers))]
    formatted = ""
    for row in table:
        formatted += " | ".join(f"{cell.center(col_widths[i])}" for i, cell in enumerate(row)) + "\n"

    return f"```\n{formatted}```"

# Wysyłanie danych do Discorda
def send_to_discord(message):
    data = {"content": message}
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code != 204:
        print(f"[ERROR] Wysyłka do Discord nie powiodła się: {response.status_code}")
    else:
        print("[OK] Wysłano wiadomość na Discord")

# Główna pętla
if __name__ == "__main__":
    entries = fetch_and_parse_logs()
    if not entries:
        print("[INFO] Brak rozpoznanych wpisów w logach.")
    else:
        save_to_db(entries)
        message = generate_table(entries)
        send_to_discord(message)
