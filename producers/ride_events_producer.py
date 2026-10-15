import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

TOPIC = "ride-events"
BOOTSTRAP_SERVERS = "localhost:29092"

RIDE_STATUSES = ["requested", "accepted", "in_progress", "completed", "cancelled"]


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def random_ride_event(ride_id: str, status: str) -> dict:
    return {
        "ride_id": ride_id,
        "rider_name": fake.name(),
        "driver_name": fake.name(),
        "status": status,
        "fare": round(random.uniform(5.0, 60.0), 2),
        "pickup_city": fake.city(),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def run():
    producer = make_producer()
    print(f"Producing fake ride events to topic '{TOPIC}'... Ctrl+C to stop.")

    try:
        while True:
            ride_id = str(uuid.uuid4())
            status = random.choice(RIDE_STATUSES)
            event = random_ride_event(ride_id, status)

            # key=ride_id makes sure all events for the SAME ride
            # land on the SAME partition, preserving their order.
            producer.send(TOPIC, key=ride_id, value=event)

            print(f"Sent: {event}")
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()