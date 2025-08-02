import re
import pandas as pd
import requests
from tabulate import tabulate
from io import StringIO
from ftplib import FTP

FTP_HOST = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

def fetch_log_content_from_ftp():
    print("🔄 Łączenie z FTP i pobieranie logów...")
    content = ""
    with FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_DIR)
        filenames = []
        ftp.retrlines("LIST", lambda line: filenames.append(line.split()[-1]))
        gameplay_logs = [f for f in filenames if f.startswith("gameplay_") and f.endswith(".log")]

        for filename in gameplay_logs:
            print(f"📄 Pobieranie pliku: {filename}")
            buffer = []
            ftp.retrbinary(f"RETR {filename}", lambda b: buffer.append(b))
            binary_content = b''.join(buffer)
            content += binary_content.decode("utf-16-le", errors="ignore") + "\n"
    print("✅ Zakończono pobieranie logów.")
    return content

def parse_log_content(content):
    pattern = re.compile(
        r"\[LogMinigame] \[LockpickingMinigame_C] User: (?P<nick>.*?) .*?Lock: (?P<lock>.*?)\..*?Success: (?P<success>Yes|No).*?Elapsed time: (?P<time>\d+\.\d+)",
        re.DOTALL
    )

    data = []
    for match in pattern.finditer(content):
        nick = match.group("nick").strip()
        lock = match.group("lock").strip()
        success = match.group("success") == "Yes"
        time = float(match.group("time"))
        data.append((nick, lock, success, time))
    return data

def create_dataframe(data):
    df = pd.DataFrame(data, columns=["Nick", "Zamek", "Sukces", "Czas"])
    if df.empty:
        return pd.DataFrame()

    grouped = df.groupby(["Nick", "Zamek"])
    result = []

    for (nick, lock), group in grouped:
        total = len(group)
        success_count = group["Sukces"].sum()
        failure_count = total - success_count
        avg_time = group["Czas"].mean()
        success_rate = (success_count / total) * 100
        result.append([
            nick,
            lock,
            total,
            success_count,
            failure_count,
            f"{success_rate:.2f}%",
            f"{avg_time:.2f} s"
        ])

    result_df = pd.DataFrame(result, columns=[
        "Nick", "Rodzaj zamka", "Wszystkie", "Udane", "Nieudane", "Skuteczność", "Średni czas"
    ])
    result_df = result_df.sort_values(by=["Nick", "Rodzaj zamka"]).reset_index(drop=True)
    return result_df

def send_to_discord(df):
    if df.empty:
        print("⚠️ Brak danych do wysłania.")
        return

    table = tabulate(df, headers="keys", tablefmt="grid", stralign="center", numalign="center")
    print("📤 Wysyłanie tabeli do Discord...")
    response = requests.post(WEBHOOK_URL, json={"content": f"```\n{table}\n```"})
    if response.status_code == 204:
        print("✅ Tabela została wysłana.")
    else:
        print(f"❌ Błąd wysyłania: {response.status_code} – {response.text}")

def main():
    content = fetch_log_content_from_ftp()
    data = parse_log_content(content)
    df = create_dataframe(data)
    send_to_discord(df)

if __name__ == "__main__":
    main()
