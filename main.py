import re
import requests
from collections import defaultdict
from statistics import mean
from datetime import timedelta

# 📌 STAŁY LOG – przykład zgodny z Twoimi wytycznymi
LOG_DATA = """
[LogMinigame] [LockpickingMinigame_C] User: Anu picked lock: easy. Success: Yes. Elapsed time: 00:01:14.542
[LogMinigame] [LockpickingMinigame_C] User: Anu picked lock: medium. Success: No. Elapsed time: 00:00:52.321
[LogMinigame] [LockpickingMinigame_C] User: Eve picked lock: easy. Success: Yes. Elapsed time: 00:00:59.000
[LogMinigame] [LockpickingMinigame_C] User: Anu picked lock: easy. Success: No. Elapsed time: 00:01:12.333
[LogMinigame] [LockpickingMinigame_C] User: Eve picked lock: easy. Success: Yes. Elapsed time: 00:00:45.600
"""

# 📌 Webhook Discord
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# 🔍 Parsowanie logów
pattern = re.compile(
    r'\[LogMinigame\] \[LockpickingMinigame_C\] User:\s*(?P<nick>\w+).*?picked lock:\s*(?P<lock>\w+)\. Success:\s*(?P<success>Yes|No)\. Elapsed time:\s*(?P<time>\d{2}:\d{2}:\d{2}\.\d{3})',
    re.DOTALL
)

results = defaultdict(list)

for match in pattern.finditer(LOG_DATA):
    nick = match['nick']
    lock = match['lock']
    success = match['success'] == 'Yes'
    h, m, s = match['time'].split(':')
    elapsed = timedelta(hours=int(h), minutes=int(m), seconds=float(s)).total_seconds()
    results[(nick, lock)].append((success, elapsed))

# 📊 Tworzenie danych do tabeli
table_data = []
for (nick, lock) in sorted(results.keys()):
    attempts = results[(nick, lock)]
    total = len(attempts)
    success_count = sum(1 for s, _ in attempts if s)
    fail_count = total - success_count
    effectiveness = f"{round(success_count / total * 100, 2)}%"
    avg_time = f"{round(mean([t for _, t in attempts]), 2)}s"
    table_data.append([
        str(nick),
        str(lock),
        str(total),
        str(success_count),
        str(fail_count),
        effectiveness,
        avg_time
    ])

headers = ["Nick", "Rodzaj zamka", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"]

# 📏 Oblicz szerokości kolumn
all_rows = [headers] + table_data
col_widths = [max(len(row[i]) for row in all_rows) for i in range(len(headers))]

# 📐 Funkcja do wyśrodkowywania komórek
def center_cell(text, width):
    padding = width - len(text)
    left = padding // 2
    right = padding - left
    return " " * left + text + " " * right

# 🧾 Budowanie wyśrodkowanej tabeli jako tekst
lines = []

# linia nagłówka
header_line = "| " + " | ".join(center_cell(h, col_widths[i]) for i, h in enumerate(headers)) + " |"
separator_line = "+-" + "-+-".join("-" * col_widths[i] for i in range(len(headers))) + "-+"
lines.append(separator_line)
lines.append(header_line)
lines.append(separator_line)

# wiersze danych
for row in table_data:
    data_line = "| " + " | ".join(center_cell(row[i], col_widths[i]) for i in range(len(headers))) + " |"
    lines.append(data_line)
lines.append(separator_line)

table_text = "```" + "\n".join(lines) + "```"

# 📤 Wysyłka do webhooka Discorda
response = requests.post(WEBHOOK_URL, json={"content": table_text})

if response.status_code == 204:
    print("✅ Tabela wysłana na Discord.")
else:
    print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")
