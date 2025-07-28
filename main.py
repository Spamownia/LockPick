import ftplib
import io
import re
import psycopg2
import requests
from collections import defaultdict

# === KONFIGURACJA ===
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# === REGEXP DOPASOWUJĄCY ZDARZENIA LOCKPICK ===
LOG_PATTERN = re.compile(
    r"User: (?P<nick>.+?) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. "
    r"Elapsed time: (?P<time>[\d.]+)\. Failed attempts: (?P<failed>\d+).+?"
    r"Lock type: (?P<lock_type>\w+)", re.MULTILINE
)

# === POŁĄCZENIE Z BAZĄ DANYCH ===
def insert_entries_to_db(entries):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lockpicks (
                id SERIAL PRIMARY KEY,
                nick TEXT,
                lock_type TEXT,
                success BOOLEAN,
                elapsed_ms INTEGER,
                failed_attempts INTEGER
            )
        """)
        for entry in entries:
            cur.execute("""
                INSERT INTO lockpicks (nick, lock_type, success, elapsed_ms, failed_attempts)
                VALUES (%s, %s, %s, %s, %s)
            """, (entry['nick'], entry['lock_type'], entry['success'], entry['elapsed_ms'], entry['failed_attempts']))
        conn.commit()
        cur.close()
        conn.close()
        print(f"[OK] Zapisano {len(entries)} wpisów do bazy.")
    except Exception as e:
        print(f"[BŁĄD DB] {e}")

# === PARSOWANIE POJEDYNCZEGO LOGU ===
def parse_log_content(content):
    matches = LOG_PATTERN.findall(content)
    parsed_entries = []
    for nick, success_str, czas_str, failed_str, lock_type in matches:
        try:
            czas_str = czas_str.strip().rstrip('.')  # ← Usuwamy końcową kropkę
            czas_ms = int(float(czas_str) * 1000)
            parsed_entries.append({
                "nick": nick,
                "lock_type": lock_type,
                "success": success_str == "Yes",
                "elapsed_ms": czas_ms,
                "failed_attempts": int(failed_str)
            })
        except Exception as e:
            print(f"[BŁĄD PARSERA] {e} → {nick}, {czas_str}, {lock_type}")
    return parsed_entries

# === TWORZENIE TABELI PODSUMOWUJĄCEJ ===
def build_summary_table(entries):
    grouped = defaultdict(list)
    for e in entries:
        key = (e["nick"], e["lock_type"])
        grouped[key].append(e)

    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table_data = [headers]

    for (nick, lock), attempts in grouped.items():
        total = len(attempts)
        success = sum(1 for a in attempts if a["success"])
        failed = total - success
        avg_time = int(sum(a["elapsed_ms"] for a in attempts) / total)
        skutecznosc = f"{(success / total * 100):.1f}%"
        row = [nick, lock, str(total), str(success), str(failed), skutecznosc, f"{avg_time} ms"]
        table_data.append(row)

    # Wyśrodkowanie i dopasowanie szerokości
    col_widths = [max(len(row[i]) for row in table_data) for i in range(len(headers))]
    table = "```\n" + "\n".join(
        " | ".join(cell.center(col_widths[i]) for i, cell in enumerate(row))
        for row in table_data
    ) + "\n```"
    return table

# === WYSYŁKA NA DISCORD ===
def send_to_discord(message):
    try:
        response = requests.post(WEBHOOK_URL, json={"content": message})
        print(f"[DISCORD] Status {response.status_code}")
    except Exception as e:
        print(f"[BŁĄD WEBHOOK] {e}")

# === POBRANIE I PARSOWANIE PLIKÓW Z FTP ===
def fetch_and_parse_logs():
    entries = []
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")
        files = ftp.nlst()
        gameplay_logs = [f for f in files if f.startswith("gameplay_")]
        print(f"[INFO] Znaleziono {len(gameplay_logs)} plików gameplay_*.log")

        for filename in gameplay_logs:
            print(f"[INFO] Przetwarzanie: {filename}")
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode("utf-16le", errors="ignore")
            parsed = parse_log_content(content)
            print(f"[DEBUG] Rozpoznano {len(parsed)} wpisów")
            entries.extend(parsed)
    return entries

# === MAIN ===
if __name__ == "__main__":
    print("[DEBUG] Start programu")
    entries = fetch_and_parse_logs()
    if not entries:
        print("[INFO] Brak rozpoznanych danych.")
    else:
        insert_entries_to_db(entries)
        tabela = build_summary_table(entries)
        send_to_discord(tabela)
