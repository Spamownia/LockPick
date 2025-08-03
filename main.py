import time from utils.parser import parse_lockpicking_log from utils.db import insert_lockpicking_entry, fetch_lockpicking_stats from utils.image_generator import generate_stats_image from utils.discord_webhook import send_webhook

LOG_FILE_PATH = "logs/latest_log.txt" WEBHOOK_URL = "https://discord.com/api/webhooks/XXX/YYY"  # <- ZMIEŃ TO

last_sent_hash = None

while True: try: entries = parse_lockpicking_log(LOG_FILE_PATH) for entry in entries: insert_lockpicking_entry(entry)

stats = fetch_lockpicking_stats()
    image_path = "output/stats.png"
    current_hash = hash(str(stats))

    if current_hash != last_sent_hash:
        generate_stats_image(stats, image_path)
        send_webhook("Lockpicking Stats", file_path=image_path, webhook_url=WEBHOOK_URL)
        last_sent_hash = current_hash

except Exception as e:
    print(f"[ERROR] {e}")

time.sleep(15)

utils/parser.py

def parse_lockpicking_log(filepath): entries = [] with open(filepath, "r", encoding="utf-16") as f: for line in f: if "[LogMinigame] [LockpickingMinigame_C]" in line: try: nick = line.split("Character=")[1].split()[0] lock = line.split("Target:")[1].split()[0] result = "Success" if "Success" in line else "Failure" time_taken = float(line.split("Time:")[1].split()[0]) entries.append({"nick": nick, "lock": lock, "result": result, "time": time_taken}) except Exception as e: print(f"[PARSE ERROR] {e} in line: {line}") return entries

utils/db.py

import psycopg

def get_conn(): return psycopg.connect( host='ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech', dbname='neondb', user='neondb_owner', password='npg_dRU1YCtxbh6v', sslmode='require', channel_binding='require' )

def insert_lockpicking_entry(entry): with get_conn() as conn: with conn.cursor() as cur: cur.execute(""" INSERT INTO lockpicking_logs (nick, lock, result, time) VALUES (%s, %s, %s, %s) """, (entry["nick"], entry["lock"], entry["result"], entry["time"]))

def fetch_lockpicking_stats(): with get_conn() as conn: with conn.cursor() as cur: cur.execute(""" SELECT nick, lock, COUNT() AS total, SUM(CASE WHEN result = 'Success' THEN 1 ELSE 0 END) AS success, SUM(CASE WHEN result = 'Failure' THEN 1 ELSE 0 END) AS failure, ROUND(SUM(CASE WHEN result = 'Success' THEN 1 ELSE 0 END)::float / COUNT() * 100, 1) AS accuracy, ROUND(AVG(time), 2) AS avg_time FROM lockpicking_logs GROUP BY nick, lock """) return cur.fetchall()

utils/image_generator.py

from PIL import Image, ImageDraw, ImageFont

def generate_stats_image(data, output_path): headers = ["Nick", "Zamek", "Próby", "Udane", "Nieudane", "Skuteczność", "Śr. czas"] font = ImageFont.load_default()

row_height = 25
padding = 10
col_widths = [max(len(str(row[i])) for row in data + [headers]) * 10 for i in range(len(headers))]
width = sum(col_widths) + padding * 2
height = (len(data) + 1) * row_height + padding * 2

img = Image.new("RGB", (width, height), "black")
draw = ImageDraw.Draw(img)

y = padding
for i, header in enumerate(headers):
    x = padding + sum(col_widths[:i])
    draw.text((x, y), header, fill="white", font=font)

y += row_height
for row in data:
    for i, cell in enumerate(row):
        x = padding + sum(col_widths[:i])
        draw.text((x, y), str(cell), fill="white", font=font)
    y += row_height

img.save(output_path)

utils/discord_webhook.py

import requests

def send_webhook(username, file_path, webhook_url): with open(file_path, "rb") as f: requests.post( webhook_url, data={"username": username}, files={"file": f} )

