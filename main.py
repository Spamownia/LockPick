import re
import csv
import io
import requests
from collections import defaultdict

# === Stały, niezmienny log (pełny z pierwszej wiadomości użytkownika) ===
log_data = """2025.07.28-08.00.38: Game version: 1.0.1.3.96391
2025.07.28-08.01.17: [LogBunkerLock] C4 Bunker Activated 03h 29m 03s ago
2025.07.28-08.01.17: [LogBunkerLock] D1 Bunker Activated 03h 29m 03s ago
2025.07.28-08.01.25: [LogBunkerLock] Bunker activations:
2025.07.28-08.01.25: [LogBunkerLock] C4 Bunker is Active. Activated 00h 00m 00s ago. X=446323.000 Y=263051.188 Z=18552.514
2025.07.28-08.01.25: [LogBunkerLock] D1 Bunker is Active. Activated 00h 00m 00s ago. X=-537889.562 Y=540004.312 Z=81279.648
2025.07.28-08.01.25: [LogBunkerLock] Locked bunkers:
2025.07.28-08.01.25: [LogBunkerLock] A3 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 20h 30m 56s. X=230229.672 Y=-447157.625 Z=9555.422
2025.07.28-08.01.25: [LogBunkerLock] A1 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 20h 30m 56s. X=-348529.312 Y=-469201.781 Z=4247.645
2025.07.28-10.01.25: [LogBunkerLock] Bunker activations:
2025.07.28-10.01.25: [LogBunkerLock] C4 Bunker is Active. Activated 00h 00m 00s ago. X=446323.000 Y=263051.188 Z=18552.514
2025.07.28-10.01.25: [LogBunkerLock] D1 Bunker is Active. Activated 00h 00m 00s ago. X=-537889.562 Y=540004.312 Z=81279.648
2025.07.28-10.01.25: [LogBunkerLock] Locked bunkers:
2025.07.28-10.01.25: [LogBunkerLock] A3 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 18h 30m 56s. X=230229.672 Y=-447157.625 Z=9555.422
2025.07.28-10.01.25: [LogBunkerLock] A1 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 18h 30m 56s. X=-348529.312 Y=-469201.781 Z=4247.645
2025.07.28-11.16.28: [LogMinigame] [LockpickingMinigame_C] User: Anu (26, 76561197992396189). Success: No. Elapsed time: 10.79. Failed attempts: 1. Target object: Improvised_Metal_Chest_C(ID: 1610220). Lock type: Advanced. User owner: 24([76561199447029491] Milo). Location: X=-377291.156 Y=-166058.812 Z=33550.902
2025.07.28-12.01.25: [LogBunkerLock] Bunker activations:
2025.07.28-12.01.25: [LogBunkerLock] C4 Bunker is Active. Activated 00h 00m 00s ago. X=446323.000 Y=263051.188 Z=18552.514
2025.07.28-12.01.25: [LogBunkerLock] D1 Bunker is Active. Activated 00h 00m 00s ago. X=-537889.562 Y=540004.312 Z=81279.648
2025.07.28-12.01.25: [LogBunkerLock] Locked bunkers:
2025.07.28-12.01.25: [LogBunkerLock] A3 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 16h 30m 56s. X=230229.672 Y=-447157.625 Z=9555.422
2025.07.28-12.01.25: [LogBunkerLock] A1 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 16h 30m 56s. X=-348529.312 Y=-469201.781 Z=4247.645
"""

# === Webhook URL ===
WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3/"

# === Regex do parsowania danych minigry ===
pattern = re.compile(
    r'\[LogMinigame\].*?User: (?P<nick>.*?) \(\d+, \d+\)\. Success: (?P<success>\w+)\. Elapsed time: (?P<time>\d+\.\d+)\..*?Lock type: (?P<lock>[\w\s]+)\.',
    re.MULTILINE
)

# === Przechowywanie statystyk ===
stats = defaultdict(lambda: {
    "all": 0,
    "success": 0,
    "fail": 0,
    "total_time": 0.0
})

# === Parsowanie logów ===
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

# === Tworzenie CSV w pamięci ===
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

# === Wysyłanie CSV na webhook Discord ===
response = requests.post(
    WEBHOOK_URL,
    files={"file": ("lockpick_stats.csv", csv_bytes, "text/csv")},
)

if response.status_code == 204:
    print("✅ Tabela CSV została pomyślnie wysłana na Discord webhook.")
else:
    print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")
