import re
import csv
import io
import requests
from collections import defaultdict

# === Stały testowy log ===
log_data = """2025.07.28-08.00.38: Game version: 1.0.1.3.96391
2025.07.28-08.01.17: [LogBunkerLock] C4 Bunker Activated 03h 29m 03s ago
2025.07.28-08.01.17: [LogBunkerLock] D1 Bunker Activated 03h 29m 03s ago
2025.07.28-11.16.28: [LogMinigame] [LockpickingMinigame_C] User: Anu (26, 76561197992396189). Success: No. Elapsed time: 10.79. Failed attempts: 1. Target object: Improvised_Metal_Chest_C(ID: 1610220). Lock type: Advanced. User owner: 24([76561199447029491] Milo). Location: X=-377291.156 Y=-166058.812 Z=33550.902
2025.07.28-11.18.10: [LogMinigame] [LockpickingMinigame_C] User: Anu (26, 76561197992396189). Success: Yes. Elapsed time: 7.53. Failed attempts: 0. Target object: Improvised_Metal_Chest_C(ID: 1610220). Lock type: Advanced. User owner: 24([76561199447029491] Milo). Location: X=-377291.156 Y=-166058.812 Z=33550.902
2025.07.28-11.20.01: [LogMinigame] [LockpickingMinigame_C] User: Anu (26, 76561197992396189). Success: Yes. Elapsed time: 5.47. Failed attempts: 0. Target object: Improvised_Metal_Chest_C(ID: 1610220). Lock type: Medium. User owner: 24([76561199447029491] Milo). Location: X=-377291.156 Y=-166058.812 Z=33550.902
2025.07.28-11.25.01: [LogMinigame] [LockpickingMinigame_C] User: Bob (44, 76561197999999999). Success: No. Elapsed time: 11.21. Failed attempts: 1. Target object: Improvised_Metal_Chest_C(ID: 1610220). Lock type: Medium. User owner: 24([76561199447029491] Milo). Location: X=-377291.156 Y=-166058.812 Z=33550.902
"""

# === Webhook ===
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3/"

# === Parsowanie wpisów minigry ===
pattern = re.compile(
    r'\[LogMinigame\].*?User: (?P<nick>.*?) \(\d+, \d+\)\. Success: (?P<success>\w+)\. Elapsed time: (?P<time>\d+\.\d+)\..*?Lock type: (?P<lock>[\w\s]+)\.',
    re.MULTILINE
)

stats = defaultdict(lambda: {
    "all": 0,
    "success": 0,
    "fail": 0,
    "total_time": 0.0
})

# === Przetwarzanie logów ===
for match in pattern.finditer(log_data):
    nick = match.group("nick").strip()
    lock = match.group("lock").strip()
    success = match.group("success").strip().lower() == "yes"
    time = float(match.group("time").strip())

    key = (nick, lock)
    stats[key]["all"] += 1
    stats[key]["success"] += int(success)
    stats[key]["fail"] += int(not success)
    stats[key]["total_time"] += time

# === Tworzenie tabeli CSV ===
output = io.StringIO()
writer = csv.writer(output)
writer.writerow(["Nick", "Rodzaj zamka", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"])

for (nick, lock) in sorted(stats.keys(), key=lambda x: (x[0].lower(), x[1].lower())):
    data = stats[(nick, lock)]
    attempts = data["all"]
    success_count = data["success"]
    fail_count = data["fail"]
    effectiveness = f"{(success_count / attempts * 100):.2f}%"
    avg_time = f"{(data['total_time'] / attempts):.2f}"

    writer.writerow([nick, lock, attempts, success_count, fail_count, effectiveness, avg_time])

csv_bytes = io.BytesIO()
csv_bytes.write(output.getvalue().encode("utf-8"))
csv_bytes.seek(0)

# === Wysyłanie pliku na webhook Discord ===
response = requests.post(
    WEBHOOK_URL,
    files={"file": ("lockpick_stats.csv", csv_bytes, "text/csv")},
)

if response.status_code == 204:
    print("✅ Tabela CSV została pomyślnie wysłana na Discord webhook.")
else:
    print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")
