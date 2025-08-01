import re
import pandas as pd
import requests
from tabulate import tabulate

# === HARDKODOWANY LOG DO TESTÓW PARSERA ===
TEST_LOG = """
[LogMinigame] [LockpickingMinigame_C] User: Anu (A1B2C3D4E5) Lock: AdvancedLock Difficulty: Advanced Success: Yes. Elapsed time: 3.45s.
[LogMinigame] [LockpickingMinigame_C] User: Anu (A1B2C3D4E5) Lock: AdvancedLock Difficulty: Advanced Success: No. Elapsed time: 6.21s.
[LogMinigame] [LockpickingMinigame_C] User: Razor (F6G7H8I9J0) Lock: BasicLock Difficulty: Basic Success: Yes. Elapsed time: 2.89s.
[LogMinigame] [LockpickingMinigame_C] User: Anu (A1B2C3D4E5) Lock: AdvancedLock Difficulty: Advanced Success: No. Elapsed time: 5.91s.
[LogMinigame] [LockpickingMinigame_C] User: Razor (F6G7H8I9J0) Lock: BasicLock Difficulty: Basic Success: No. Elapsed time: 4.11s.
"""

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

def parse_log_content(content):
    print("[DEBUG] Rozpoczynam parsowanie logów...")

    pattern = re.compile(
        r"\[LogMinigame\]\s+\[LockpickingMinigame_C\]\s+User:\s+(?P<nick>.+?)\s+\(.+?\)\s+Lock:\s+(?P<lock>.+?)\s+Difficulty:\s+(?P<difficulty>.+?)\s+Success:\s+(?P<success>Yes|No)\.\s+Elapsed time:\s+(?P<time>\d+\.\d+)s"
    )

    data = []
    for match in pattern.finditer(content):
        entry = {
            "Nick": match.group("nick").strip(),
            "Zamek": match.group("lock").strip(),
            "Sukces": match.group("success").strip(),
            "Czas": float(match.group("time").strip()),
        }
        print(f"[DEBUG] Rozpoznano wpis: {entry}")
        data.append(entry)

    print(f"[DEBUG] Liczba poprawnie sparsowanych wpisów: {len(data)}")
    return data

def create_dataframe(parsed_data):
    df = pd.DataFrame(parsed_data)
    if df.empty:
        print("[DEBUG] Brak danych do przetworzenia.")
        return None

    grouped = df.groupby(["Nick", "Zamek"]).agg(
        Próby=("Sukces", "count"),
        Udane=("Sukces", lambda x: (x == "Yes").sum()),
        Nieudane=("Sukces", lambda x: (x == "No").sum()),
        Skuteczność=("Sukces", lambda x: round((x == "Yes").mean() * 100, 2)),
        Średni_czas=("Czas", lambda x: round(x.mean(), 2)),
    ).reset_index()

    print("[DEBUG] Tabela statystyk została utworzona.")
    return grouped

def format_table_for_discord(df):
    headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]
    table = tabulate(
        df.values.tolist(),
        headers=headers,
        tablefmt="github",
        stralign="center",
        numalign="center"
    )
    return f"```markdown\n{table}\n```"

def send_to_discord(message):
    print("[DEBUG] Wysyłam dane na Discord...")
    response = requests.post(WEBHOOK_URL, json={"content": message})
    if response.status_code == 204:
        print("[DEBUG] Wysłano pomyślnie.")
    else:
        print(f"[ERROR] Błąd wysyłania: {response.status_code} - {response.text}")

def main():
    print("[DEBUG] Start przetwarzania loga testowego")
    parsed = parse_log_content(TEST_LOG)
    df = create_dataframe(parsed)
    if df is not None:
        table_message = format_table_for_discord(df)
        send_to_discord(table_message)
    else:
        print("[DEBUG] Nie wygenerowano tabeli - brak danych.")

if __name__ == "__main__":
    main()
