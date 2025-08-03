import os
import ftplib
import re
import io
import psycopg2
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
import requests
from flask import Flask

# === KONFIGURACJA ===
FTP_HOST = '176.57.174.10'
FTP_PORT = 50021
FTP_USER = 'gpftp37275281717442833'
FTP_PASS = 'LXNdGShY'
FTP_DIR = '/SCUM/Saved/SaveFiles/Logs/'

PGHOST = 'ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech'
PGDATABASE = 'neondb'
PGUSER = 'neondb_owner'
PGPASSWORD = 'npg_dRU1YCtxbh6v'
PGSSLMODE = 'require'
PGCHANNELBINDING = 'require'

DISCORD_WEBHOOK = 'https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3'

app = Flask(__name__)

def get_latest_log_file(ftp):
    ftp.cwd(FTP_DIR)
    files = ftp.nlst()
    gameplay_logs = sorted([f for f in files if f.startswith("gameplay_") and f.endswith(".log")])
    return gameplay_logs[-1] if gameplay_logs else None

def parse_log_file(ftp, log_filename):
    data = []
    ftp.retrbinary(f'RETR {FTP_DIR}{log_filename}', lambda b: process_log_lines(b, data))
    return data

def process_log_lines(binary_data, data):
    content = binary_data.decode('windows-1250', errors='ignore')
    for line in content.splitlines():
        line = line.strip()
        if 'LockpickingMinigame_C' in line:
            match = re.search(
                r'User: (.*?) \(\d+, (\d+)\).*?Success: (Yes|No).*?Elapsed time: ([\d.]+).*?Failed attempts: (\d+).*?Target object: (.*?)\(ID: \d+\).*?Lock type: (\w+).*?User owner: \d+\(\[(\d+)\] (.*?)\).*?Location: X=([-.\d]+) Y=([-.\d]+) Z=([-.\d]+)',
                line
            )
            if match:
                user, steamid, success, time, fails, target, locktype, ownerid, owner, x, y, z = match.groups()
                data.append({
                    "user": user,
                    "steamid": steamid,
                    "success": success == "Yes",
                    "time": float(time),
                    "fails": int(fails),
                    "target": target,
                    "locktype": locktype,
                    "owner": owner,
                    "x": float(x),
                    "y": float(y),
                    "z": float(z),
                })

def save_to_postgres(entries):
    conn = psycopg2.connect(
        host=PGHOST,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        sslmode=PGSSLMODE,
        options='-c channel_binding=require'
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lockpicks (
            id SERIAL PRIMARY KEY,
            username TEXT,
            steamid TEXT,
            success BOOLEAN,
            elapsed_time FLOAT,
            failed_attempts INT,
            target TEXT,
            locktype TEXT,
            owner TEXT,
            x FLOAT,
            y FLOAT,
            z FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    for e in entries:
        cur.execute("""
            INSERT INTO lockpicks (username, steamid, success, elapsed_time, failed_attempts, target, locktype, owner, x, y, z)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (e["user"], e["steamid"], e["success"], e["time"], e["fails"], e["target"], e["locktype"], e["owner"], e["x"], e["y"], e["z"]))
    conn.commit()
    cur.close()
    conn.close()

def generate_stats_image():
    conn = psycopg2.connect(
        host=PGHOST,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        sslmode=PGSSLMODE,
        options='-c channel_binding=require'
    )
    df = pd.read_sql_query("SELECT username, target, success, elapsed_time FROM lockpicks", conn)
    conn.close()

    if df.empty:
        return None

    grouped = df.groupby(["username", "target"]).agg(
        Attempts=('success', 'count'),
        Successes=('success', 'sum'),
        AvgTime=('elapsed_time', 'mean')
    )
    grouped["Fails"] = grouped["Attempts"] - grouped["Successes"]
    grouped["Accuracy"] = (grouped["Successes"] / grouped["Attempts"] * 100).round(1)
    grouped["AvgTime"] = grouped["AvgTime"].round(2)
    grouped.reset_index(inplace=True)
    grouped = grouped[["username", "target", "Attempts", "Successes", "Fails", "Accuracy", "AvgTime"]]

    img_width, img_height = 1000, 40 + 30 * (len(grouped) + 1)
    img = Image.new('RGB', (img_width, img_height), color='white')
    draw = ImageDraw.Draw(img)
    font = ImageFont.load_default()

    headers = ["Nick", "Zamek", "Próby", "Udane", "Nieudane", "Skuteczność (%)", "Śr. czas (s)"]
    x_positions = [20, 180, 400, 500, 600, 720, 880]
    for i, header in enumerate(headers):
        draw.text((x_positions[i], 10), header, font=font, fill='black')

    for idx, row in grouped.iterrows():
        y = 40 + idx * 30
        values = [row["username"], row["target"], str(row["Attempts"]), str(row["Successes"]), str(row["Fails"]), f"{row['Accuracy']}%", str(row["AvgTime"])]
        for i, val in enumerate(values):
            draw.text((x_positions[i], y), val, font=font, fill='black')

    output = io.BytesIO()
    img.save(output, format='PNG')
    output.seek(0)
    return output

def send_to_discord(image_data):
    if image_data is None:
        return
    files = {'file': ('stats.png', image_data, 'image/png')}
    requests.post(DISCORD_WEBHOOK, files=files)

@app.route('/')
def index():
    return 'LockpickingLogger is running'

def run_bot():
    print("🚀 Start bota LockpickingLogger...")
    print("🔍 Skanowanie wszystkich logów przy starcie...")
    print("🔗 Łączenie z FTP (bez TLS)...")
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)

    latest_log = get_latest_log_file(ftp)
    if not latest_log:
        print("❌ Brak pliku gameplay_*.log.")
        return

    entries = parse_log_file(ftp, latest_log)
    if not entries:
        print("ℹ️ Brak nowych wpisów lockpickingu.")
        return

    save_to_postgres(entries)
    image = generate_stats_image()
    send_to_discord(image)

    ftp.quit()
    print("✅ Gotowe. Dane zapisane i wysłane.")

if __name__ == '__main__':
    run_bot()
    app.run(host='0.0.0.0', port=8080)
