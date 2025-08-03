main.py

import os import time from utils.parser import parse_lockpicking_log from utils.db import insert_entries, fetch_lockpicking_stats from utils.image_generator import generate_table_image from utils.discord_webhook import send_webhook

LAST_IMAGE_HASH = ""

def file_hash(path): import hashlib with open(path, "rb") as f: return hashlib.md5(f.read()).hexdigest()

def main_loop(): global LAST_IMAGE_HASH LOG_PATH = "logs/latest_log.txt" IMAGE_PATH = "output/lock_stats.png"

if not os.path.exists("output"):
    os.makedirs("output")

while True:
    if not os.path.exists(LOG_PATH):
        print("Brak logu.")
        time.sleep(15)
        continue

    try:
        entries = parse_lockpicking_log(LOG_PATH)
        insert_entries(entries)
        stats = fetch_lockpicking_stats()
        generate_table_image(stats, IMAGE_PATH)

        current_hash = file_hash(IMAGE_PATH)
        if current_hash != LAST_IMAGE_HASH:
            send_webhook(
                username="SCUM LockpickBot",
                content="\ud83d\udd10 Statystyki lockpickingu:",
                file_path=IMAGE_PATH,
                webhook_url="https://discord.com/api/webhooks/..."  # <--- Wstaw swój webhook
            )
            LAST_IMAGE_HASH = current_hash
            print("\ud83d\udfe2 Wysłano nowy obrazek na Discord.")
        else:
            print("\ud83d\udd01 Brak zmian — nic nie wysyłamy.")
    except Exception as e:
        print("\u274c Błąd:", e)

    time.sleep(15)

if name == "main": main_loop()

utils/parser.py

import re

def parse_lockpicking_log(filepath): pattern = re.compile( r'   (?P<nick>\w+) (?P<result>FAILED|SUCCESSFULLY) lockpicking (?P<lock>\w+).*?in (?P<time>\d+.\d+) seconds' )

entries = []

with open(filepath, "r", encoding="utf-8") as f:
    for line in f:
        match = pattern.search(line)
        if match:
            entries.append({
                "nick": match.group("nick"),
                "lock": match.group("lock"),
                "result": "Success" if match.group("result") == "SUCCESSFULLY" else "Failure",
                "time": float(match.group("time"))
            })
return entries

utils/db.py

import psycopg

DB_CONFIG = { "host": "ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech", "dbname": "neondb", "user": "neondb_owner", "password": "npg_dRU1YCtxbh6v", "sslmode": "require", "channel_binding": "require" }

def insert_entries(entries): if not entries: return with psycopg.connect(**DB_CONFIG) as conn: with conn.cursor() as cur: for entry in entries: cur.execute(""" INSERT INTO lockpicking_logs (nick, lock, result, time) VALUES (%s, %s, %s, %s) """, (entry['nick'], entry['lock'], entry['result'], entry['time'])) conn.commit()

def fetch_lockpicking_stats(): with psycopg.connect(**DB_CONFIG) as conn: with conn.cursor() as cur: cur.execute(""" SELECT nick, lock, COUNT() AS total_attempts, SUM(CASE WHEN result = 'Success' THEN 1 ELSE 0 END) AS successes, SUM(CASE WHEN result = 'Failure' THEN 1 ELSE 0 END) AS failures, ROUND(SUM(CASE WHEN result = 'Success' THEN 1 ELSE 0 END)::float / COUNT() * 100, 1) AS accuracy, ROUND(AVG(time), 2) AS avg_time FROM lockpicking_logs GROUP BY nick, lock ORDER BY accuracy DESC """) return cur.fetchall()

utils/image_generator.py

from PIL import Image, ImageDraw, ImageFont

def generate_table_image(data, output_path): headers = ["Nick", "Zamek", "Ilość wszystkich prób", "Udane", "Nieudane", "Skuteczność", "Średni czas"] font = ImageFont.truetype("arial.ttf", 20)

col_widths = [max(len(str(val)) for val in [h] + [row[i] for row in data]) * 14 for i, h in enumerate(headers)]

total_width = sum(col_widths) + len(headers) * 10
row_height = 40
img_height = (len(data) + 1) * row_height + 20

image = Image.new("RGB", (total_width, img_height), "black")
draw = ImageDraw.Draw(image)

y = 10
x = 0
for i, h in enumerate(headers):
    draw.text((x + 5, y), h, font=font, fill="white")
    x += col_widths[i] + 10

y += row_height
for row in data:
    x = 0
    for i, val in enumerate(row):
        draw.text((x + 5, y), str(val), font=font, fill="white")
        x += col_widths[i] + 10
    y += row_height

image.save(output_path)

utils/discord_webhook.py

import requests

def send_webhook(username, content, file_path, webhook_url): with open(file_path, 'rb') as f: files = {'file': f} data = { 'username': username, 'content': content } requests.post(webhook_url, data=data, files=files)

