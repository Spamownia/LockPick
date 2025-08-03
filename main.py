main.py

import os import time from utils.parser import parse_lockpicking_log from utils.db import insert_entries, fetch_lockpicking_stats from utils.image_generator import generate_table_image from utils.discord_webhook import send_webhook

LOG_FILE_PATH = "logs/latest_log.txt" WEBHOOK_URL = "https://discord.com/api/webhooks/XXX/YYY"  # <-- Podmień na swój webhook

last_sent_hash = None

while True: try: entries = parse_lockpicking_log(LOG_FILE_PATH) insert_entries(entries)

stats = fetch_lockpicking_stats()
    image_path = "output/stats.png"
    current_hash = hash(str(stats))

    if current_hash != last_sent_hash:
        generate_table_image(stats, image_path)
        send_webhook("Lockpicking Stats", file_path=image_path, webhook_url=WEBHOOK_URL)
        last_sent_hash = current_hash

except Exception as e:
    print(f"[ERROR] {e}")

time.sleep(15)

utils/parser.py

def parse_lockpicking_log(filepath): import re

entries = []
pattern = re.compile(r"LogMinigame] \[LockpickingMinigame_C (.*?) lock (.*?) result: (Success|Failure) in ([\d.]+)s")

with open(filepath, "r", encoding="utf-16") as f:
    for line in f:
        match = pattern.search(line)
        if match:
            nick, lock, result, time_str = match.groups()
            entries.append({
                "nick": nick,
                "lock": lock,
                "result": result,
                "time": float(time_str)
            })
return entries

utils/db.py

import psycopg2 import os

conn = psycopg2.connect( host="ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", dbname="neondb", user="neondb_owner", password="npg_dRU1YCtxbh6v", sslmode="require", channel_binding="require" )

def insert_entries(entries): with conn.cursor() as cur: for e in entries: cur.execute(""" INSERT INTO lockpicking_logs (nick, lock, result, time) VALUES (%s, %s, %s, %s) """, (e["nick"], e["lock"], e["result"], e["time"])) conn.commit()

def fetch_lockpicking_stats(): with conn.cursor() as cur: cur.execute(""" SELECT nick, lock, COUNT() AS total, SUM(CASE WHEN result = 'Success' THEN 1 ELSE 0 END) AS success, SUM(CASE WHEN result = 'Failure' THEN 1 ELSE 0 END) AS failure, ROUND(100.0 * SUM(CASE WHEN result = 'Success' THEN 1 ELSE 0 END) / COUNT(), 1) AS accuracy, ROUND(AVG(time), 2) AS avg_time FROM lockpicking_logs GROUP BY nick, lock ORDER BY total DESC """) return cur.fetchall()

utils/image_generator.py

from PIL import Image, ImageDraw, ImageFont

def generate_table_image(rows, save_path): headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"] font = ImageFont.load_default()

# Oblicz szerokości kolumn
col_widths = [max(len(str(row[i])) for row in rows + [headers]) * 10 for i in range(len(headers))]
row_height = 25
img_width = sum(col_widths) + 20
img_height = (len(rows) + 1) * row_height + 20

img = Image.new("RGB", (img_width, img_height), color=(30, 30, 30))
draw = ImageDraw.Draw(img)

y = 10
for i, header in enumerate(headers):
    x = sum(col_widths[:i]) + 10
    draw.text((x, y), header, fill="white", font=font)

y += row_height
for row in rows:
    for i, cell in enumerate(row):
        x = sum(col_widths[:i]) + 10
        draw.text((x, y), str(cell), fill="white", font=font)
    y += row_height

img.save(save_path)

utils/discord_webhook.py

import requests

def send_webhook(username, file_path=None, webhook_url=None): with open(file_path, "rb") as f: files = {"file": f} data = {"username": username} response = requests.post(webhook_url, data=data, files=files) 
response.raise_for_status()
