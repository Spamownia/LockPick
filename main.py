import os import re import psycopg2 import requests from flask import Flask from collections import defaultdict

Konfiguracja webhooka

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

Konfiguracja bazy danych

DB_CONFIG = { "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", "dbname": "neondb", "user": "neondb_owner", "password": "npg_dRU1YCtxbh6v", "sslmode": "require" }

def create_stats_table(): with psycopg2.connect(**DB_CONFIG) as conn: with conn.cursor() as cur: cur.execute(""" CREATE TABLE IF NOT EXISTS lockpick_stats ( nick TEXT, lock_type TEXT, total_attempts INTEGER, successes INTEGER, failures INTEGER, average_time FLOAT ) """) conn.commit() print("[DB] Tabela lockpick_stats sprawdzona lub utworzona.")

def insert_stats(stats): with psycopg2.connect(**DB_CONFIG) as conn: with conn.cursor() as cur: for (nick, lock_type), data in stats.items(): total = data['success'] + data['fail'] avg_time = sum(data['times']) / len(data['times']) if data['times'] else 0.0 cur.execute(""" INSERT INTO lockpick_stats (nick, lock_type, total_attempts, successes, failures, average_time) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (nick, lock_type) DO UPDATE SET total_attempts = EXCLUDED.total_attempts, successes = EXCLUDED.successes, failures = EXCLUDED.failures, average_time = EXCLUDED.average_time """, (nick, lock_type, total, data['success'], data['fail'], avg_time)) conn.commit() print("[DB] Wstawiono statystyki do lockpick_stats.")

def parse_log_line(line): match = re.search( r"User: (.?) .?. Success: (Yes|No). Elapsed time: ([\d.]+). Failed attempts: \d+. .*?Lock type: (\w+)", line ) if match: nick = match.group(1) success = match.group(2) == "Yes" time = float(match.group(3)) lock_type = match.group(4) return nick, lock_type, success, time return None

def generate_stats_table(stats): headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"] rows = [] for (nick, lock_type), data in stats.items(): total = data['success'] + data['fail'] success_rate = f"{(data['success'] / total * 100):.1f}%" if total else "0.0%" avg_time = f"{sum(data['times']) / len(data['times']):.2f}" if data['times'] else "0.00" rows.append([nick, lock_type, str(total), str(data['success']), str(data['fail']), success_rate, avg_time])

# Znajdź maksymalną szerokość kolumn
col_widths = [max(len(row[i]) for row in [headers] + rows) for i in range(len(headers))]
def format_row(row):
    return " | ".join(cell.center(col_widths[i]) for i, cell in enumerate(row))

table = "```

" + format_row(headers) + "\n" + "-+-".join("-" * w for w in col_widths) + "\n" table += "\n".join(format_row(row) for row in rows) + "\n```" return table

def send_to_webhook(content): response = requests.post(WEBHOOK_URL, json={"content": content}) if response.status_code == 204: print("[WEBHOOK] Wysłano dane na Discorda.") else: print(f"[WEBHOOK] Błąd wysyłania: {response.status_code} - {response.text}")

def analyze_logs(): # Przykładowy log log_lines = [ "2025.07.20-13.09.52: [LogMinigame] [LockpickingMinigame_C] User: Anu (26, 76561197992396189). Success: Yes. Elapsed time: 6.63. Failed attempts: 0. Target object: BPLockpick_Weapon_Locker_Military_C(ID: N/A). Lock type: VeryEasy. User owner: N/A. Location: X=229088.922 Y=-437256.875 Z=8386.837", ]

stats = defaultdict(lambda: {"success": 0, "fail": 0, "times": []})
for line in log_lines:
    parsed = parse_log_line(line)
    if parsed:
        nick, lock_type, success, time = parsed
        key = (nick, lock_type)
        if success:
            stats[key]["success"] += 1
        else:
            stats[key]["fail"] += 1
        stats[key]["times"].append(time)
if stats:
    insert_stats(stats)
    table = generate_stats_table(stats)
    send_to_webhook(table)
else:
    print("[INFO] Brak rozpoznanych danych w logu.")

app = Flask(name)

@app.route("/") def index(): return "Alive"

if name == "main": print("[DEBUG] Uruchamianie aplikacji Flask...") create_stats_table() analyze_logs() port = int(os.environ.get("PORT", 10000)) app.run(host="0.0.0.0", port=port)

