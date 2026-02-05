import json
import time
import random
from datetime import datetime
import boto3
import os

os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""
os.environ["AWS_DEFAULT_REGION"] = ""

kinesis = boto3.client(
    "kinesis",
    region_name="eu-north-1"
)



while True:
    event = {
        "user_id": f"user_{random.randint(1,50)}",
        "event_type": "page_view",
        "page": random.choice(["/home","/search","/checkout"]),
        "device": random.choice(["mobile","web"]),
        "event_time": datetime.utcnow().isoformat()
    }

    kinesis.put_record(
        StreamName="clickstream-events",
        Data=json.dumps(event),
        PartitionKey=event["user_id"]
    )

    print(event)
    time.sleep(1)
