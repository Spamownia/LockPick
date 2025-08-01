import re
import pandas as pd
from datetime import datetime, timezone
from flask import Flask
import requests

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

LOG_MINIGAME_PATTERN = re.compile(
    r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}: \[LogMinigame\] \[LockpickingMinigame_C\] User: (?P<nick>\w+) \(\d+, \d+\)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<elapsed_time>[\d.]+)\. Failed attempts: (?P<failed_attempts>\d+)\. Target object: [\w_]+\(ID: \d+\)\. Lock type: (?P<lock_type>\w+)\. .*"
)

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

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
    summary = summary.sort_values(by=["Nick", "Zamek"])
    return summary

def format_table_for_webhook(df):
    headers_display = ["Nick", "Zamek", "Próby", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    headers_df = ["Nick", "Zamek", "Próby", "Udane", "Nieudane", "Skuteczność", "Średni_czas"]

    col_widths = []
    for col in headers_df:
        max_len = max(df[col].astype(str).map(len).max(), len(headers_display[headers_df.index(col)]))
        col_widths.append(max_len)

    def center_text(text, width):
        text = str(text)
        space = width - len(text)
        left = space // 2
        right = space - left
        return " " * left + text + " " * right

    header_line = "| " + " | ".join(center_text(h, w) for h, w in zip(headers_display, col_widths)) + " |"
    separator_line = "|-" + "-|-".join("-" * w for w in col_widths) + "-|"

    rows = []
    for _, row in df.iterrows():
        row_line = "| " + " | ".join(center_text(row[col], w) for col, w in zip(headers_df, col_widths)) + " |"
        rows.append(row_line)

    table_text = "\n".join([header_line, separator_line] + rows)
    return table_text

def send_to_discord(content):
    data = {"content": f"```\n{content}\n```"}
    response = requests.post(WEBHOOK_URL, json=data)
    if response.status_code != 204:
        print(f"[ERROR] Błąd wysyłki webhook: {response.status_code} - {response.text}")
    else:
        print("[DEBUG] Wysłano tabelę na webhook Discord.")

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
        table_text = format_table_for_webhook(df)
        print("[DEBUG] Tabela do wysłania na webhook:")
        print(table_text)
        send_to_discord(table_text)
    else:
        print("[DEBUG] Brak danych do wyświet
