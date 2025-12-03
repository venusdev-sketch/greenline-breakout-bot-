# test_telegram_send.py
# Local debug: uses notify_config.json (keeps token local)
import json, requests, os, sys, time

cfg_path = "notify_config.json"
if not os.path.exists(cfg_path):
    print("ERROR: notify_config.json not found")
    sys.exit(1)

cfg = json.load(open(cfg_path, "r", encoding="utf-8"))
tcfg = cfg.get("telegram", {})
token = tcfg.get("bot_token")
chat_id = tcfg.get("chat_id")

print("Using chat_id:", chat_id)
if not token:
    print("ERROR: bot_token missing in config")
    sys.exit(2)

base = f"https://api.telegram.org/bot{token}"
print("Calling getMe...")
try:
    r = requests.get(base + "/getMe", timeout=15)
    print("getMe HTTP:", r.status_code)
    print("getMe JSON:", r.text)
except Exception as e:
    print("getMe request failed:", e)

print("\nSending test message...")
payload = {"chat_id": chat_id, "text": "GLB scanner test message - ignore", "parse_mode": "HTML"}
try:
    r2 = requests.post(base + "/sendMessage", data=payload, timeout=20)
    print("sendMessage HTTP:", r2.status_code)
    print("sendMessage JSON:", r2.text)
except Exception as e:
    print("sendMessage request failed:", e)

print("\nDone.")
