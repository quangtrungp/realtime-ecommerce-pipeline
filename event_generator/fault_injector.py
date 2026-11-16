
import random
import copy
from datetime import datetime, timedelta, timezone


def inject_duplicates(events, probability=0.02):

    result = []

    for event in events:

        result.append(event)

        if random.random() < probability:

            # Copy the original event
            duplicate = copy.deepcopy(event)

            result.append(duplicate)

    return result

def inject_late_events(events, probability=0.03):

    result = []

    for event in events:

        late_event = copy.deepcopy(event)

        if random.random() < probability:

            event_time = datetime.fromisoformat(
                late_event["event_time"].replace("Z", "+00:00")
            )

            late_event["event_time"] = (
                event_time - timedelta(minutes=5)
            ).isoformat(
                timespec="milliseconds"
            ).replace("+00:00", "Z")

        result.append(late_event)

    return result

def inject_invalid_events(events, probability=0.01):

    result = []

    for event in events:

        invalid_event = copy.deepcopy(event)

        if random.random() < probability:

            event_type = invalid_event["event_type"]
            payload = invalid_event["payload"]

            if event_type == "order_created":

                if payload["items"]:
                    payload["items"][0]["quantity"] = 0

            elif event_type.startswith("payment_"):

                payload["amount"] = "-1000.00"

        result.append(invalid_event)

    return result

def reorder_events(events, probability=0.05):

    grouped_events = {}

    for event in events:

        order_id = event["payload"]["order_id"]

        if order_id not in grouped_events:
            grouped_events[order_id] = []

        grouped_events[order_id].append(event)

    result = []

    for order_id, order_events in grouped_events.items():

        order_events = copy.deepcopy(order_events)

        for i in range(len(order_events) - 1):

            if random.random() < probability:

                # Swap two adjacent events
                order_events[i], order_events[i + 1] = (
                    order_events[i + 1],
                    order_events[i]
                )

        result.extend(order_events)

    return result