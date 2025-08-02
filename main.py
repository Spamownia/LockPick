import re
import requests
import pandas as pd
from tabulate import tabulate

# Stały log testowy (dokładnie z pierwszej wiadomości)
LOG_CONTENT = """
2025.07.28-08.00.38: Game version: 1.0.1.3.96391
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
2025.07.28-11.16.28: [LogMinigame] [LockpickingMinigame_C] User: Anu (26, 76561197992396189). Success: No. Elapsed time: 10.00. Failed attempts: 1. Target object: Improvised_Metal_Chest_C(ID: 1610220). Lock type: Advanced. User owner: 24([76561199447029491] Milo). Location: X=-377291.156 Y=-166058.812 Z=33550.902
2025.07.28-11.16.28: [LogMinigame] [LockpickingMinigame_C] User: Anu (26, 76561197992396189). Success: Yes. Elapsed time: 5.00. Failed attempts: 1. Target object: Improvised_Metal_Chest_C(ID: 1610220). Lock type: Advanced. User owner: 24([76561199447029491] Milo). Location: X=-377291.156 Y=-166058.812 Z=33550.902
2025.07.28-12.01.25: [LogBunkerLock] Bunker activations:
2025.07.28-12.01.25: [LogBunkerLock] C4 Bunker is Active. Activated 00h 00m 00s ago. X=446323.000 Y=263051.188 Z=18552.514
2025.07.28-12.01.25: [LogBunkerLock] D1 Bunker is Active. Activated 00h 00m 00s ago. X=-537889.562 Y=540004.312 Z=81279.648
2025.07.28-12.01.25: [LogBunkerLock] Locked bunkers:
2025.07.28-12.01.25: [LogBunkerLock] A3 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 16h 30m 56s. X=230229.672 Y=-447157.625 Z=9555.422
2025.07.28-12.01.25: [LogBunkerLock] A1 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 16h 30m 56s. X=-348529.312 Y=-469201.781 Z=4247.645
"""

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

def parse_log_minigame(log_text):
    print("🔍 Rozpoczynam parsowanie loga...")
    pattern = re.compile(
        r"\[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<user>\w+).*?Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d\.]+)\. .*?Lock type: (?P<lock>\w+)\."
    )
    matches = pattern.finditer(log_text)
    data = []
    count = 0
    for match in matches:
        count += 1
        user = match.group("user")
        success = match.group("success") == "Yes"
        time = float(match.group("time"))
        lock = match.group("lock")
        print(f"  • Wpis #{count}: Użytkownik={user}, Sukces={success}, Czas={time}, Rodzaj zamka={lock}")
        data.append((user, lock, success, time))
    print(f"Parsowanie zakończone, znaleziono {count} wpisów.")
    return data

def analyze_data(entries):
    print("📊 Analizuję dane...")
    df = pd.DataFrame(entries, columns=["Nick", "Zamek", "Sukces", "Czas"])
    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Wszystkie=("Sukces", "count"),
        Udane=("Sukces", "sum"),
        Nieudane=("Sukces", lambda x: (~x).sum()),
        Średni_czas=("Czas", "mean"),
    )
    grouped["Skuteczność"] = (grouped["Udane"] / grouped["Wszystkie"] * 100).round(1).astype(str) + "%"
    grouped["Średni_czas"] = grouped["Średni_czas"].round(2).astype(str) + "s"
    grouped = grouped.reset_index()
    grouped = grouped.sort_values(by=["Nick", "Zamek"])
    print("Analiza zakończona. Oto podsumowanie:")
    print(grouped)
    return grouped[["Nick", "Zamek", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni_czas"]]

def format_table(df):
    print("📝 Tworzę tabelę markdown z wyśrodkowaniem...")
    table = tabulate(
        df.values,
        headers=df.columns,
        tablefmt="github",
        stralign="center",
        numalign="center"
    )
    print("Tabela gotowa.")
    return f"```\n{table}\n```"

def send_to_discord(content):
    print("🚀 Wysyłam tabelę na Discord webhook...")
    response = requests.post(WEBHOOK_URL, json={"content": content})
    if response.status_code in (200, 204):
        print("✅ Wysłano pomyślnie.")
    else:
        print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")

if __name__ == "__main__":
    parsed_entries = parse_log_minigame(LOG_CONTENT)
    analyzed_df = analyze_data(parsed_entries)
    table_text = format_table(analyzed_df)
    send_to_discord(table_text)
