import os import re import io import time import ssl import pandas as pd import psycopg2 from ftplib import FTP from datetime import datetime from tabulate import tabulate import requests from flask import Flask

--- KONFIGURACJE ---

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"

DB_CONFIG = { 'host': "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", 'dbname': "neondb", 'user': "neondb_owner", 'password': "npg_dRU1YCtxbh6v", 'sslmode': "require" }

--- FUNKCJE ---

def connect_ftp(): ftp = FTP() ftp.connect(FTP_HOST, FTP_PORT) ftp.login(FTP_USER, FTP_PASS) ftp.cwd(FTP_DIR) return ftp

def list_log_files(): ftp = connect_ftp() files = [] ftp.retrlines('LIST', lambda line: files.append(line)) filenames = [line.split()[-1] for line in files if line.split()[-1].startswith("gameplay_") and line.endswith(".log")] ftp.quit() return filenames

def download_file(filename): ftp = connect_ftp() bio = io.BytesIO() ftp.retrbinary(f"RETR {filename}", bio.write) ftp.quit() bio.seek(0) return bio.read().decode('utf-16-le', errors='ignore')

def parse_log_content(content): pattern = re.compile( r"[LogMinigame] [LockpickingMinigame_C] User: (?P<nick>.?) .?Lock: (?P<lock>.?) Success: (?P<success>Yes|No).?Elapsed time: (?P<time>\d+(.\d+)?)", re.DOTALL ) return pattern.findall(content)

def aggregate_entries(entries): stats = {} for nick, lock, success, time_str, _ in entries: key = (nick, lock) if key not in stats: stats[key] = {'total': 0, 'success': 0, 'fail': 0, 'times': []} stats[key]['total'] += 1 if success == "Yes": stats[key]['success'] += 1 else: stats[key]['fail'] += 1 stats[key]['times'].append(float(time_str)) return stats

def create_dataframe(stats): lock_order = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4} rows = [] for (nick, lock), values in stats.items(): effectiveness = round(100 * values['success'] / values['total'], 2) avg_time = round(sum(values['times']) / len(values['times']), 2) rows.append([nick, lock, values['total'], values['success'], values['fail'], effectiveness, avg_time]) df = pd.DataFrame(rows, columns=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"]) df["Zamek"] = pd.Categorical(df["Zamek"], categories=["VeryEasy", "Basic", "Medium", "Advanced", "DialLock"], ordered=True) df.sort_values(by=["Nick", "Zamek"], inplace=True) return df

def send_to_discord(df): if df.empty: print("[DEBUG] Brak danych do wysłania.") return table = tabulate(df.values.tolist(), headers=df.columns, tablefmt="grid", stralign="center") print("[DEBUG] Tabela do wysłania:\n" + table) payload = { "content": f"{table}" } response = requests.post(WEBHOOK_URL, json=payload) print("[DEBUG] Wysłano na webhook, status:", response.status_code)

def download_and_parse_logs(): files = list_log_files() all_entries = [] for file in files: print(f"[DEBUG] Przetwarzanie pliku: {file}") content = download_file(file) matches = parse_log_content(content) print(f"[DEBUG] Rozpoznano {len(matches)} wpisów w {file}") all_entries.extend(matches) return all_entries

def main_loop(): print("[DEBUG] Start programu") entries = download_and_parse_logs() if not entries: print("[DEBUG] Brak nowych wpisów w logach") return stats = aggregate_entries(entries) df = create_dataframe(stats) send_to_discord(df)

--- HTTP PING ---

app = Flask(name)

@app.route('/') def index(): return "Alive"

if name == 'main': main_loop() app.run(host='0.0
.0.0', port=3000)
