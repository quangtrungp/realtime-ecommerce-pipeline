
import json
from confluent_kafka import Producer

import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)

from event_generator.main import main

order_topic = "order-events"
payment_topic = "payment-events"

producer = Producer({
    "bootstrap.servers": "localhost:9092",
})


def on_delivery(error, message):
    if error:
        print(f"Gửi thất bại: {error}")
    else:
        print(
            f"Đã ghi: key={message.key().decode()} "
            f"topic={message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )


events = main(5)

order_events = [
    event for event in events
    if event["event_type"].startswith("order_")
]

payment_events = [
    event for event in events
    if event["event_type"].startswith("payment_")
]


for event in order_events:
    key = event["payload"]["order_id"].encode("utf-8")
    value = json.dumps(event, ensure_ascii=False).encode("utf-8")

    producer.produce(
        topic=order_topic,
        key=key,
        value=value,
        callback=on_delivery,
    )

for event in payment_events:
    key = event["payload"]["order_id"].encode("utf-8")
    value = json.dumps(event, ensure_ascii=False).encode("utf-8")

    producer.produce(
        topic=payment_topic,
        key=key,
        value=value,
        callback=on_delivery,
    )

remaining = producer.flush(timeout=30)

if remaining > 0:
    raise RuntimeError(f"Còn {remaining} message chưa được gửi")
