from confluent_kafka import Producer
import json
import os

# ── Kafka Producer (singleton) ────────────────────────────────────
_producer = None


def get_producer():
    """Get or create the shared Kafka producer."""
    global _producer
    if _producer is None:
        _producer = Producer({
            'bootstrap.servers': 'localhost:9092'
        })
    return _producer


def send_event(topic: str, key: str, data: dict):
    """Send an event to a Kafka topic.

    Args:
        topic: The Kafka topic name (e.g. "order-placed")
        key:   The message key (e.g. order number)
        data:  The event payload as a dictionary
    """
    producer = get_producer()

    def on_delivery(error, message):
        if error:
            print(f"  ❌ Kafka delivery failed: {error}")
        else:
            print(f"  ✅ Event sent to '{message.topic()}' "
                  f"partition={message.partition()} offset={message.offset()}")

    producer.produce(
        topic=topic,
        key=str(key),
        value=json.dumps(data),
        callback=on_delivery
    )
    producer.poll(0)
    producer.flush()

    print(f"📤 Event published: topic='{topic}' key='{key}'")