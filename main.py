import os
import io
import re
import ftplib
import psycopg2
import requests
from collections import defaultdict

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}


def download_logs():
    entries = []
    with ftplib.FTP() as ftp:
        print("[DEBUG] Connecting to FTP...")
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_PATH)

        files = []
        ftp.retrlines("LIST", lambda x: files.append(x))
        log_filenames = [line.split()[-1] for line in files if line.split()[-1].startswith("gameplay_")]

        print(f"[DEBUG] Found {len(log_filenames)} gameplay logs")

        for filename in log_filenames:
            print(f"[DEBUG] Downloading {filename}...")
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {filename}", bio.write)
            content = bio.getvalue().decode("utf-16-le", errors="ignore")
            parsed = parse_log_content(content)
            entries.extend(parsed)
            print(f"[DEBUG] Parsed {len(parsed)} entries from {filename}")

    return entries


def parse_log_content(content):
    pattern = re.compile(
        r"User: (?P<nick>.+?) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<elapsed>\d+\.\d+)"
        r"\. Failed attempts: (?P<fails>\d+).+?Lock type: (?P<locktype>\w+)", re.DOTALL
    )
    entries = []
    for match in pattern.finditer(content):
        entries.append({
            "nick": match.group("nick"),
            "success": match.group("success") == "Yes",
            "elapsed": float(match.group("elapsed")),
            "fails": int(match.group("fails")),
            "locktype": match.group("locktype")
        })
    return entries


def save_to_db(entries):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS lockpicking_logs (id SERIAL PRIMARY KEY);")
            required_columns = {
                "nick": "TEXT",
                "locktype": "TEXT",
                "success": "BOOLEAN",
                "elapsed": "FLOAT",
                "fails": "INTEGER"
            }
            for col, coltype in required_columns.items():
                try:
                    cur.execute(f"ALTER TABLE lockpicking_logs ADD COLUMN {col} {coltype};")
                except psycopg2.errors.DuplicateColumn:
                    conn.rollback()

            for e in entries:
                cur.execute("""
                    INSERT INTO lockpicking_logs (nick, locktype, success, elapsed, fails)
                    VALUES (%s, %s, %s, %s, %s);
                """, (e["nick"], e["locktype"], e["success"], e["elapsed"], e["fails"]))
        conn.commit()
        print(f"[DEBUG] Saved {len(entries)} entries to DB")


def fetch_summary():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nick, locktype,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE success) AS successes,
                       COUNT(*) FILTER (WHERE NOT success) AS failures,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE success) / NULLIF(COUNT(*), 0), 2) AS accuracy,
                       ROUND(AVG(elapsed), 2) AS avg_time
                FROM lockpicking_logs
                GROUP BY nick, locktype
                ORDER BY nick, locktype;
            """)
            return cur.fetchall()


def format_table(data):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    col_widths = [len(h) for h in headers]

    rows = []
    for row in data:
        str_row = [str(v) for v in row]
        for i, v in enumerate(str_row):
            col_widths[i] = max(col_widths[i], len(v))
        rows.append(str_row)

    def format_row(r):
        return " | ".join(v.center(col_widths[i]) for i, v in enumerate(r))

    header_line = format_row(headers)
    separator = "-+-".join("-" * w for w in col_widths)
    lines = [header_line, separator] + [format_row(r) for r in rows]
    return "```\n" + "\n".join(lines) + "\n```"


def send_to_discord(table_text):
    print("[DEBUG] Sending summary to Discord...")
    response = requests.post(WEBHOOK_URL, json={"content": table_text})
    print(f"[DEBUG] Discord response: {response.status_code}")


def main():
    print("[DEBUG] Start main()")
    entries = download_logs()
    if not entries:
        print("[DEBUG] No entries parsed.")
        return
    save_to_db(entries)
    summary = fetch_summary()
    table_text = format_table(summary)
    send_to_discord(table_text)
    print("[DEBUG] Done.")


if __name__ == "__main__":
    main()
