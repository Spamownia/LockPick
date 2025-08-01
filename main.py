import re
import pandas as pd
from datetime import datetime, timezone
from flask import Flask

app = Flask(__name__)

# Dokładnie taki log, jaki przesłałeś:
TEST_LOG = """
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
2025.07.28-11.16.28: [LogMinigame] [LockpickingMinigame_C] User: Anu (26, 76561197992396189). Success: No. Elapsed time: 10.79. Failed attempts: 1. Target object: Improvised_Metal_Chest_C(ID: 1610220). Lock type: Advanced. User owner: 24([76561199447029491] Milo). Location: X=-377291.156 Y=-166058.812 Z=33550.902
2025.07.28-12.01.25: [LogBunkerLock] Bunker activations:
2025.07.28-12.01.25: [LogBunkerLock] C4 Bunker is Active. Activated 00h 00m 00s ago. X=446323.000 Y=263051.188 Z=18552.514
2025.07.28-12.01.25: [LogBunkerLock] D1 Bunker is Active. Activated 00h 00m 00s ago. X=-537889.562 Y=540004.312 Z=81279.648
2025.07.28-12.01.25: [LogBunkerLock] Locked bunkers:
2025.07.28-12.01.25: [LogBunkerLock] A3 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 16h 30m 56s. X=230229.672 Y=-447157.625 Z=9555.422
2025.07.28-12.01.25: [LogBunkerLock] A1 Bunker is Locked. Locked 00h 00m 00s ago, next Activation in 16h 30m 56s. X=-348529.312 Y=-469201.781 Z=4247.645
"""

# Regex parsujący wpisy LogMinigame według podanego wzorca:
LOG_MINIGAME_PATTERN = re.compile(
    r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}: \[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>\w+) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<elapsed_time>[\d.]+)\. Failed attempts: (?P<failed_attempts>\d+)\. Target object: [\w_]+\(ID: \d+\)\. Lock type: (?P<lock_type>\w+)\. .*"
)

def parse_log_content(log_content):
    entries = []
    for line in log_content.splitlines():
        match = LOG_MINIGAME_PATTERN.match(line)
        if match:
            nick = match.group("nick")
            lock_type = match.group("lock_type")
            success = match.group("success") == "Yes"
            elapsed_time = float(match.group("elapsed_time"))
            failed_attempts = int(match.group("failed_attempts"))
            entries.append({
                "Nick": nick,
                "Zamek": lock_type,
                "Success": success,
                "Elapsed_time": elapsed_time,
                "Failed_attempts": failed_attempts
            })
    return entries

def create_dataframe(entries):
    if not entries:
        return pd.DataFrame()
    df = pd.DataFrame(entries)
    summary = df.groupby(["Nick", "Zamek"]).agg(
        Próby=pd.NamedAgg(column="Success", aggfunc="count"),
        Udane=pd.NamedAgg(column="Success", aggfunc="sum"),
        Nieudane=pd.NamedAgg(column="Success", aggfunc=lambda x: (~x).sum()),
        Średni_czas=pd.NamedAgg(column="Elapsed_time", aggfunc="mean")
    ).reset_index()
    summary["Skuteczność"] = (summary["Udane"] / summary["Próby"] * 100).round(2)
    summary["Średni_czas"] = summary["Średni_czas"].round(2)
    return summary

def main_loop_test():
    print("[DEBUG] Baza danych zainicjalizowana.")
    print("[DEBUG] Start main_loop (tryb testowy)")
    print(f"[DEBUG] Pętla sprawdzania {datetime.now(timezone.utc)}")

    entries = parse_log_content(TEST_LOG)
    print(f"[DEBUG] Rozpoznano {len(entries)} wpisów w logu.")
    
    df = create_dataframe(entries)
    if not df.empty:
        print("[DEBUG] Stworzono DataFrame:")
        print(df.to_string(index=False))
    else:
        print("[DEBUG] Brak danych do wyświetlenia.")

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    main_loop_test()
    app.run(host="0.0.0.0", port=3000)
