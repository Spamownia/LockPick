import ftplib
import psycopg2
import re
import time
import requests
from collections import defaultdict

# --- Konfiguracja ---
FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

# --- Pobranie listy plików z FTP bez użycia nlst() ---
def fetch_log_files_ftp(ftp):
    files = []
    def parse_line(line):
        parts = line.split(None, 8)
        if len(parts) == 9:
            filename = parts[-1]
            # Filtrujemy tylko pliki gameplay_*.log, jeśli takie są, lub inne odpowiednie
            if filename.startswith("gameplay_") and filename.endswith(".log"):
                files.append(filename)
    ftp.retrlines("LIST", parse_line)
    return files

# --- Pobranie i dekodowanie plików logów z FTP ---
def fetch_logs():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)
    print("[INFO] Połączono z FTP.")
    files = fetch_log_files_ftp(ftp)
    logs = []
    for file in files:
        lines = []
        ftp.retrlines(f"RETR {file}", lines.append)
        # Pliki kodowane UTF-16 LE, dekodujemy
        raw_bytes = "\n".join(lines).encode('latin1')  # ftp.retrlines zwraca już tekst, więc by odkodować, musimy pobrać raw? Niestety retrlines zwraca już dekodowany tekst.
        # Dlatego zamiast retrlines użyjemy retrbinary
        # Zmieniamy podejście do pobierania zawartości:
    ftp.quit()

    # Poprawione pobieranie zawartości plików UTF-16 LE:
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)
    logs = []
    for file in files:
        raw_data = []
        ftp.retrbinary(f"RETR {file}", raw_data.append)
        raw_bytes = b"".join(raw_data)
        text = raw_bytes.decode("utf-16le")
        logs.append(text)
    ftp.quit()
    print(f"[DEBUG] Liczba logów: {len(logs)}")
    return logs

# --- Parsowanie logów, wyciąganie wpisów Lockpicking ---
def parse_logs(log_texts):
    entries = []
    # Regex dopasowujący linie lockpick
    pattern = re.compile(
        r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}: \[LogMinigame\] \[LockpickingMinigame_C\] "
        r"User: (?P<nick>.+?) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. "
        r"Elapsed time: (?P<time>[\d\.]+)\. Failed attempts: \d+\. "
        r"Target object: .*? Lock type: (?P<lock_type>\w+)\..*"
    )

    for text in log_texts:
        for line in text.splitlines():
            match = pattern.match(line)
            if match:
                entry = {
                    "nick": match.group("nick"),
                    "castle": match.group("lock_type"),
                    "result": match.group("success"),
                    "time": match.group("time").rstrip("."),  # usuwamy kropkę na końcu jeśli jest
                }
                entries.append(entry)
    print(f"[DEBUG] Sparsowane wpisy lockpick: {len(entries)}")
    return entries

# --- Sprawdzenie czy wpis już istnieje ---
def entry_exists(cur, entry):
    cur.execute("""
        SELECT 1 FROM lockpick_entries 
        WHERE nick=%s AND castle=%s AND result=%s AND time=%s
        """, (entry["nick"], entry["castle"], entry["result"], float(entry["time"])))
    return cur.fetchone() is not None

# --- Zapis wpisów do bazy ---
def save_entries(cur, conn, entries):
    new_entries = 0
    for entry in entries:
        if not entry_exists(cur, entry):
            cur.execute("""
                INSERT INTO lockpick_entries (nick, castle, result, time) 
                VALUES (%s, %s, %s, %s)
            """, (entry["nick"], entry["castle"], entry["result"], float(entry["time"])))
            new_entries += 1
    conn.commit()
    print(f"[DEBUG] Nowe wpisy: {new_entries}")
    return new_entries

# --- Pobranie statystyk z bazy ---
def get_stats(cur):
    cur.execute("""
        SELECT nick, castle, 
               COUNT(*) as total,
               COUNT(*) FILTER (WHERE result='Yes') as success,
               COUNT(*) FILTER (WHERE result='No') as fail,
               AVG(time) as avg_time
        FROM lockpick_entries
        GROUP BY nick, castle
        ORDER BY nick
    """)
    return cur.fetchall()

# --- Formatowanie i wysłanie tabeli do webhook ---
def send_webhook(stats):
    # Przygotowanie danych do tabeli
    header = ["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    rows = []
    for row in stats:
        nick, castle, total, success, fail, avg_time = row
        success_ratio = (success / total * 100) if total > 0 else 0
        rows.append([
            nick, 
            castle, 
            str(total), 
            str(success), 
            str(fail), 
            f"{success_ratio:.1f}%", 
            f"{avg_time:.2f}"
        ])

    # Obliczamy szerokość kolumn (max długość)
    columns = list(zip(*([header] + rows)))
    col_widths = [max(len(str(cell)) for cell in col) for col in columns]

    # Formatowanie wierszy z wyśrodkowaniem
    def format_row(row):
        return "| " + " | ".join(f"{str(cell):^{col_widths[i]}}" for i, cell in enumerate(row)) + " |"

    table = "\n".join([
        format_row(header),
        "|-" + "-|-".join('-' * w for w in col_widths) + "-|"
    ] + [format_row(row) for row in rows])

    payload = {"content": f"```\n{table}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)
    if response.ok:
        print("[INFO] Wysłano tabelę na webhook.")
    else:
        print(f"[ERROR] Błąd wysyłania webhook: {response.status_code} - {response.text}")

# --- Inicjalizacja bazy danych ---
def init_db():
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        sslmode=DB_CONFIG["sslmode"]
    )
    cur = conn.cursor()
    # Tworzymy tabelę jeśli nie istnieje
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpick_entries (
            id SERIAL PRIMARY KEY,
            nick TEXT NOT NULL,
            castle TEXT NOT NULL,
            result TEXT NOT NULL,
            time REAL NOT NULL
        );
    """)
    conn.commit()
    return conn, cur

# --- Główna funkcja ---
def main():
    print("[INFO] Inicjalizacja bazy...")
    conn, cur = init_db()

    print("[INFO] Pobieranie logów...")
    logs = fetch_logs()

    print("[INFO] Parsowanie danych...")
    entries = parse_logs(logs)

    if not entries:
        print("[INFO] Brak nowych danych w logach.")
        conn.close()
        return

    print("[INFO] Zapisywanie do bazy...")
    new_count = save_entries(cur, conn, entries)

    if new_count > 0:
        print("[INFO] Pobieranie statystyk...")
        stats = get_stats(cur)
        send_webhook(stats)
    else:
        print("[INFO] Brak nowych danych – webhook nie wysłany.")

    conn.close()

if __name__ == "__main__":
    main()
