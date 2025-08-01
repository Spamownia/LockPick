import re
import csv
import io
import requests
from collections import defaultdict
from statistics import mean
from datetime import timedelta

# 📌 STAŁY LOG – zgodny z wytycznymi
LOG_DATA = """
[LogMinigame] [LockpickingMinigame_C] User: Anu picked lock: easy. Success: Yes. Elapsed time: 00:01:14.542
[LogMinigame] [LockpickingMinigame_C] User: Anu picked lock: medium. Success: No. Elapsed time: 00:00:52.321
[LogMinigame] [LockpickingMinigame_C] User: Eve picked lock: easy. Success: Yes. Elapsed time: 00:00:59.000
[LogMinigame] [LockpickingMinigame_C] User: Anu picked lock: easy. Success: No. Elapsed time: 00:01:12.333
[LogMinigame] [LockpickingMinigame_C] User: Eve picked lock: easy. Success: Yes. Elapsed time: 00:00:45.600
"""

# 📌 DISCORD WEBHOOK
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

# 🔍 Parsowanie logów (struktur: [LogMinigame] [LockpickingMinigame_C] ...)
pattern = re.compile(
    r'\[LogMinigame\] \[LockpickingMinigame_C\] User:\s*(?P<nick>\w+).*?picked lock:\s*(?P<lock>\w+)\. Success:\s*(?P<success>Yes|No)\. Elapsed time:\s*(?P<time>\d{2}:\d{2}:\d{2}\.\d{3})',
    re.DOTALL
)

results = defaultdict(list)

for match in pattern.finditer(LOG_DATA):
    nick = match['nick']
    lock = match['lock'].strip()
    success = match['success'] == 'Yes'
    elapsed_str = match['time']
    h, m, s = elapsed_str.split(':')
    elapsed = timedelta(hours=int(h), minutes=int(m), seconds=float(s)).total_seconds()

    key = (nick, lock)
    results[key].append((success, elapsed))

# 📊 Generowanie CSV
output = io.StringIO()
writer = csv.writer(output)
writer.writerow(['Nick', 'Zamek', 'Wszystkie próby', 'Udane', 'Nieudane', 'Skuteczność', 'Średni czas'])

for (nick, lock) in sorted(results.keys()):
    attempts = results[(nick, lock)]
    total = len(attempts)
    success_count = sum(1 for success, _ in attempts if success)
    fail_count = total - success_count
    effectiveness = f"{round(success_count / total * 100, 2)}%"
    avg_time = f"{round(mean([t for _, t in attempts]), 2)}s"
    writer.writerow([nick, lock, total, success_count, fail_count, effectiveness, avg_time])

csv_data = output.getvalue()

# 📤 Wysyłanie CSV jako plik do Discord webhook
response = requests.post(
    WEBHOOK_URL,
    files={"file": ("lockpicking_stats.csv", csv_data, "text/csv")}
)

if response.status_code == 204:
    print("✅ CSV wysłany na Discord.")
else:
    print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")
