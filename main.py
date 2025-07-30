import os import re import time import pandas as pd import psycopg2 import requests from tabulate import tabulate from ftplib import FTP_TLS from io import BytesIO, StringIO from flask import Flask

--- Flask app for uptime checks ---

app = Flask(name)

@app.route('/') def index(): return "Alive"

--- Konfiguracja ---

FTP_HOST = "176.57.174.10" FTP_PORT = 50021 FTP_USER = "gpftp37275281717442833" FTP_PASS = "LXNdGShY" FTP_DIR = "/SCUM/Saved/SaveFiles/Logs/" WEBHOOK_URL = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3" DB_CONFIG = { 'host': "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", 'dbname': "neondb", 'user': "neondb_owner", 'password': "npg_dRU1YCtxbh6v", 'sslmode': "require" }

PROCESSED_LINES = {}

--- Funkcja: Pobieranie listy plików ---

def list_log_files(): with FTP_TLS() as ftps: ftps.connect(FTP_HOST, FTP_PORT) ftps.login(FTP_USER, FTP_PASS) ftps.prot_p() ftps.cwd(FTP_DIR) files = [] ftps.retrlines('LIST', lambda line: files.append(line.split()[-1])) log_files = [f for f in files if f.startswith("gameplay_") and f.endswith(".log")] print(f"[DEBUG] Znalezione pliki logów: {log_files}") return log_files

--- Funkcja: Parsowanie treści loga ---

def parse_log_content(content): pattern = re.compile(r"  User: (?P<nick>.?) .?Type: (?P<type>.*?)\. Success: (?P<success>Yes|No)\. Elapsed time: (?P<time>[\d.]+)") results = [] for match in pattern.finditer(content): results.append({ "Nick": match.group("nick"), "Zamek": match.group("type"), "Sukces": match.group("success") == "Yes", "Czas": float(match.group("time")) }) print(f"[DEBUG] Przetworzono {len(results)} wpisów z loga") return results

--- Funkcja: Tworzenie DataFrame ---

def create_dataframe(entries): df = pd.DataFrame(entries) if df.empty: print("[DEBUG] Brak danych do analizy w tym przebiegu") return None grouped = df.groupby(["Nick", "Zamek"]).agg( Proby=('Sukces', 'count'), Udane=('Sukces', 'sum'), Nieudane=(lambda x: (~x).sum()), Skutecznosc=(lambda x: round(x.sum() / len(x) * 100, 2)), SredniCzas=('Czas', 'mean') ).reset_index() grouped['SredniCzas'] = grouped['SredniCzas'].round(2) print("[DEBUG] Stworzono podsumowanie tabeli:") print(grouped) return grouped

--- Funkcja: Wysyłka tabeli na Discord ---

def send_to_discord(df): if df is None or df.empty: print("[DEBUG] Brak danych do wysyłki na Discord") return table = tabulate(df, headers=["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"], tablefmt="github", showindex=False) message = f"Statystyki Lockpicków:\n\n{table}\n" response = requests.post(WEBHOOK_URL, json={"content": message}) print(f"[DEBUG] Wysłano tabelę do Discorda. Status: {response.status_code}")

--- Funkcja: Zapis danych do bazy ---

def save_to_database(entries): if not entries: print("[DEBUG] Brak danych do zapisu w bazie") return try: conn = psycopg2.connect(**DB_CONFIG) cur = conn.cursor() cur.execute(""" CREATE TABLE IF NOT EXISTS lockpick_stats ( nick TEXT, zamek TEXT, sukces BOOLEAN, czas FLOAT, timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP ) """) for entry in entries: cur.execute(""" INSERT INTO lockpick_stats (nick, zamek, sukces, czas) VALUES (%s, %s, %s, %s) """, (entry['Nick'], entry['Zamek'], entry['Sukces'], entry['Czas'])) conn.commit() cur.close() conn.close() print(f"[DEBUG] Zapisano {len(entries)} wpisów do bazy") except Exception as e: print(f"[ERROR] Błąd zapisu do bazy: {e}")

--- Główna pętla ---

def main_loop(): print("[DEBUG] Start main_loop") while True: try: files = list_log_files() all_new_entries = [] for filename in files: with FTP_TLS() as ftps: ftps.connect(FTP_HOST, FTP_PORT) ftps.login(FTP_USER, FTP_PASS) ftps.prot_p() ftps.cwd(FTP_DIR) bio = BytesIO() ftps.retrbinary(f"RETR {filename}", bio.write) bio.seek(0) content = bio.read().decode("utf-16-le", errors='ignore') lines = content.splitlines()

if filename not in PROCESSED_LINES:
                    PROCESSED_LINES[filename] = 0

                new_lines = lines[PROCESSED_LINES[filename]:]
                new_content = "\n".join(new_lines)
                entries = parse_log_content(new_content)
                if entries:
                    all_new_entries.extend(entries)
                    PROCESSED_LINES[filename] = len(lines)
                else:
                    print(f"[DEBUG] Brak nowych wpisów w pliku {filename}")

        if all_new_entries:
            save_to_database(all_new_entries)
            df = create_dataframe(all_new_entries)
            send_to_discord(df)
        else:
            print("[DEBUG] Brak nowych danych do przetworzenia w tej iteracji")

    except Exception as e:
        print(f"[ERROR] Błąd główny: {e}")

    time.sleep(60)

--- Uruchomienie ---

if name == "main": from threading import Thread Thread(target=main_loop).start() app.run(ho
st='0.0.0.0', port=3000)
