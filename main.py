import ftplib
import io
import re
import psycopg2
import requests
from collections import defaultdict

# --- KONFIGURACJA ---
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

# --- PARSER ---
log_entry_pattern = re.compile(
    r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>.*?) \(\d+, \d+\)\. "
    r"Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>\d+(\.\d+)?)\. "
    r"Failed attempts: (?P<fail>\d+)\. Target object: .*?\. Lock type: (?P<locktype>\w+)\."
)

def parse_log_content(content):
    stats = defaultdict(lambda: {
        "total": 0, "success": 0, "fail": 0, "sum_time": 0.0
    })

    for match in log_entry_pattern.finditer(content):
        nick = match.group("nick").strip()
        locktype = match.group("locktype").strip()
        success = match.group("success") == "Yes"
        time_str = match.group("time").strip().rstrip(".")
        try:
            elapsed = float(time_str)
        except ValueError:
            continue

        key = (nick, locktype)
        stats[key]["total"] += 1
        stats[key]["sum_time"] += elapsed
        if success:
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1

    return stats

# --- TABELA ---
def build_table(stats):
    headers = ["Nick", "Zamek", "Ilość prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    rows = []

    for (nick, locktype), data in stats.items():
        total = data["total"]
        success = data["success"]
        fail = data["fail"]
        avg_time = round(data["sum_time"] / total, 2) if total else 0
        acc = f"{(success / total * 100):.1f}%" if total else "0%"
        rows.append([nick, locktype, str(total), str(success), str(fail), acc, str(avg_time)])

    col_widths = [max(len(row[i]) for row in [headers] + rows) for i in range(len(headers))]

    def format_row(row):
        return " | ".join(cell.center(col_widths[i]) for i, cell in enumerate(row))

    table = "```\n" + format_row(headers) + "\n" + "-+-".join("-" * w for w in col_widths) + "\n"
    for row in rows:
        table += format_row(row) + "\n"
    return table + "```"

# --- DANE FTP ---
def fetch_and_parse_logs():
    all_stats = defaultdict(lambda: {
        "total": 0, "success": 0, "fail": 0, "sum_time": 0.0
    })

    print("[DEBUG] Start programu")
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        print(f"[OK] Połączono z FTP: {FTP_HOST}:{FTP_PORT}")

        ftp.cwd(FTP_LOG_DIR)
        filenames = []
        ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
        log_files = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]

        print(f"[INFO] Znaleziono {len(log_files)} plików gameplay_*.log")
        for filename in log_files:
            print(f"[INFO] Przetwarzanie: {filename}")
            with io.BytesIO() as bio:
                ftp.retrbinary(f"RETR {filename}", bio.write)
                content = bio.getvalue().decode("utf-16-le", errors="ignore")
                stats = parse_log_content(content)
                for key, val in stats.items():
                    for k in val:
                        all_stats[key][k] += val[k]

    return all_stats

# --- DB ---
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicking_stats (
            nick TEXT,
            locktype TEXT,
            total INT,
            success INT,
            fail INT,
            avg_time FLOAT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def save_to_db(stats):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM lockpicking_stats")  # reset przed zapisem

    for (nick, locktype), data in stats.items():
        avg_time = round(data["sum_time"] / data["total"], 2) if data["total"] else 0.0
        cur.execute("""
            INSERT INTO lockpicking_stats (nick, locktype, total, success, fail, avg_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (nick, locktype, data["total"], data["success"], data["fail"], avg_time))

    conn.commit()
    cur.close()
    conn.close()

# --- MAIN ---
if __name__ == "__main__":
    init_db()
    stats = fetch_and_parse_logs()
    if stats:
        save_to_db(stats)
        tabela = build_table(stats)
        print("[OK] Wygenerowano tabelę:\n", tabela)
        requests.post(WEBHOOK_URL, json={"content": tabela})
    else:
        print("[INFO] Brak danych do przetworzenia.")
