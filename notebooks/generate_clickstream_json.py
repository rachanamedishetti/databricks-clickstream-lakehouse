"""
import json
import random
import time
from datetime import datetime

# base_path = "/dbfs/mnt/clickstream/raw/"
base_path = "/Volumes/clickstream_prod/analytics/raw_events/"

events = ["page_view", "add_to_cart", "purchase"]
pages = ["/", "/home", "/product", "/cart"]

for i in range(10):
    event = {
        "user_id": f"user_{random.randint(1, 10)}",
        "event_type": random.choice(events),
        "page": random.choice(pages),
        "device": random.choice(["mobile", "desktop"]),
        "event_time": datetime.utcnow().isoformat()
    }

    file_path = f"{base_path}event_{int(time.time())}_{i}.json"

    with open(file_path, "w") as f:
        json.dump(event, f)

    print("Written:", file_path)
    time.sleep(1)
"""
import json
import random
import time
from datetime import datetime, timedelta
import uuid
import os

base_path = "/Volumes/clickstream_prod/analytics/raw_events/"
os.makedirs(base_path, exist_ok=True)

event_types = ["page_view", "add_to_cart", "purchase", "INVALID_EVENT"]

def generate_event(i):
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1,5)}",
        "event_type": random.choice(event_types),
        "event_time": datetime.utcnow().isoformat(),
        "device": random.choice(["mobile", "web"]),
        "price": random.choice([10, 20, None])
    }

    # 10% missing user_id
    if random.random() < 0.1:
        del event["user_id"]

    # 10% corrupt timestamp
    if random.random() < 0.1:
        event["event_time"] = 123456

    # 10% late events
    if random.random() < 0.1:
        event["event_time"] = (datetime.utcnow() - timedelta(days=7)).isoformat()

    return event

# Generate files
for i in range(20):
    file_path = f"{base_path}/event_{int(time.time())}_{i}.json"
    with open(file_path, "w") as f:
        json.dump(generate_event(i), f)

    print("Written:", file_path)
    time.sleep(1)
