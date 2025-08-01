import re
import pandas as pd
import psycopg2
import time
import requests
from tabulate import tabulate
from flask import Flask
from datetime import datetime, UTC
import threading

# === Konfiguracja bazy danych ===
DB_CONFIG = {
    "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_dRU1YCtxbh6v",
    "sslmode": "require"
}

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# === Przykładowy testowy log zaszyty na sztywno ===
TEST_LOG_CONTENT = """
[LogMinigame] [LockpickingMinigame_C] User: Anu Lock: Medium Success: Yes. Elapsed time: 2.3s
[LogMinigame] [LockpickingMinigame_C] User: Anu Lock: Medium Success: No. Elapsed time: 3.5s
[LogMinigame] [LockpickingMinigame_C] User: Rex Lock: Easy Success: Yes. Elapsed time: 1.2s
"""

# === Parser logów ===
def parse_log_content(content):
    entries = []
    pattern = re.compile(
        r'\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>\w+) Lock: (?P<lock>[\w\s]+) Success: (?P<success>Yes|No). Elapsed time: (?P<time>[\d.]+)s'
    )
    for match in pattern.finditer(content):
        nick = match.group("nick")
        lock = match.group("lock")
        success = match.group("success")
        elapsed_time = float(match.group("time"))
        entries.append({
            "Nick": nick,
            "Zamek": lock,
            "Sukces": success == "Yes",
            "Czas": elapsed_time
        })
    print(f"[DEBUG] Rozpoznano {len(entries)} wpisów w logu.")
    return entries

# === Tworzenie i aktualizacja danych do tabeli ===
def create_dataframe(entries):
    if not entries:
        print("[DEBUG] Brak danych do analizy.")
        return pd.DataFrame()

    df = pd.DataFrame(entries)
    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Próby=("Sukces", "count"),
        Udane=("Sukces", "sum"),
        Nieudane=("Sukces", lambda x: (~x).sum()),
        Skuteczność=("Sukces", lambda x: round(100 * x.sum() / x.count(), 2)),
        Średni_czas=("Czas", lambda x: round(x.mean(), 2))
    ).reset_index()

    print("[DEBUG] Stworzono DataFrame:")
    print(grouped)
    return grouped

# === Wysyłka do Discorda ===
def send_to_discord(df):
    if df.empty:
        print("[DEBUG] Brak danych do wysłania.")
        return

    df.columns = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

    table_str = tabulate(df, headers="keys", tablefmt="github", stralign="center", numalign="center")
    payload = {"content": f"```\n{table_str}\n```"}
    response = requests.post(WEBHOOK_URL, json=payload)

    print(f"[DEBUG] Wysłano do Discorda (status: {response.status_code})")

# === Inicjalizacja bazy (placeholder) ===
def init_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lock_stats (
                id SERIAL PRIMARY KEY,
                nick TEXT,
                lock TEXT,
                success BOOLEAN,
                time FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("[DEBUG] Baza danych zainicjalizowana.")
    except Exception as e:
        print(f"[ERROR] Błąd inicjalizacji bazy danych: {e}")

# === Główna pętla przetwarzania (testowa) ===
def main_loop():
    print("[DEBUG] Start main_loop (tryb testowy)")
    while True:
        print(f"[DEBUG] Pętla sprawdzania {datetime.now(UTC)}")
        entries = parse_log_content(TEST_LOG_CONTENT)
        df = create_dataframe(entries)
        send_to_discord(df)
        print("[DEBUG] Oczekiwanie 60s...\n")
        time.sleep(60)

# === Flask do pingowania ===
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

# === Uruchomienie wątku głównego ===
if __name__ == "__main__":
    init_db()
    threading.Thread(target=main_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=3000)
