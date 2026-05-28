from confluent_kafka import Consumer
import json
import signal
import os
import sys
from datetime import datetime, timezone
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

# ── Configuration ─────────────────────────────────────────────────
TOPICS = [
    'order-placed',
    'payment-received',
    'item-shipped',
    'item-delivered',
]

# Where to write the Parquet files
DATA_LAKE_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'data-lake'
)

# How often to flush buffered events to Parquet files
FLUSH_INTERVAL_SECONDS = 30
FLUSH_BATCH_SIZE = 50  # also flush if buffer hits this size

# ── Kafka Consumer ────────────────────────────────────────────────
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'lake-writer',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe(TOPICS)

print("🏔️  Lake Writer Service is running!")
print(f"👂 Listening on topics: {', '.join(TOPICS)}")
print(f"📁 Writing Parquet files to: {DATA_LAKE_ROOT}")
print(f"⏱️  Flush interval: every {FLUSH_INTERVAL_SECONDS}s or {FLUSH_BATCH_SIZE} events")
print("   Press Ctrl+C to stop\n")

# ── Event Buffer ──────────────────────────────────────────────────
# Events are grouped by topic, then flushed to Parquet periodically
buffers = defaultdict(list)
event_counter = 0

# ── Graceful Shutdown ─────────────────────────────────────────────
running = True

def stop(_sig, _frame):
    global running
    print("\n⚠️  Stopping Lake Writer...")
    running = False

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def flush_buffer(topic, events):
    """Write a list of events to a Parquet file, partitioned by date."""
    if not events:
        return

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    output_dir = os.path.join(DATA_LAKE_ROOT, topic, today)
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime('%H%M%S_%f')
    filename = f"batch_{timestamp}.parquet"
    filepath = os.path.join(output_dir, filename)

    rows = []
    for event in events:
        rows.append({
            'event_topic': event['_topic'],
            'event_key': event['_key'],
            'event_partition': event['_partition'],
            'event_offset': event['_offset'],
            'event_timestamp': event['_ingested_at'],
            'event_data': json.dumps(event['_payload']),
        })

    df = pd.DataFrame(rows)
    df.to_parquet(filepath, engine='fastparquet', index=False)
    print(f"  💾 Wrote {len(events)} events → {filepath}")


def flush_all_buffers():
    """Flush all topic buffers to Parquet files."""
    global event_counter
    for topic, events in buffers.items():
        if events:
            flush_buffer(topic, events)
    buffers.clear()
    event_counter = 0


# ── Main Loop ─────────────────────────────────────────────────────
import time
last_flush_time = time.time()

try:
    while running:
        message = consumer.poll(timeout=1.0)

        if message is None:
            # No message — check if it's time to flush anyway
            if time.time() - last_flush_time >= FLUSH_INTERVAL_SECONDS and event_counter > 0:
                print(f"\n⏱️  Flush interval reached ({FLUSH_INTERVAL_SECONDS}s)")
                flush_all_buffers()
                last_flush_time = time.time()
            continue

        if message.error():
            print(f"❌ Error: {message.error()}")
            continue

        raw_value = message.value()
        if raw_value is None:
            continue

        # Parse the event
        payload = json.loads(raw_value.decode('utf-8'))
        topic = message.topic()
        key = message.key().decode('utf-8') if message.key() else ''

        # Wrap with metadata
        event = {
            '_topic': topic,
            '_key': key,
            '_partition': message.partition(),
            '_offset': message.offset(),
            '_ingested_at': datetime.now(timezone.utc).isoformat(),
            '_payload': payload,
        }

        # Add to buffer
        buffers[topic].append(event)
        event_counter += 1

        print(f"📥 Buffered event #{event_counter}: "
              f"topic={topic} key={key} offset={message.offset()}")

        # Flush if batch size reached
        if event_counter >= FLUSH_BATCH_SIZE:
            print(f"\n📦 Batch size reached ({FLUSH_BATCH_SIZE} events)")
            flush_all_buffers()
            last_flush_time = time.time()

finally:
    # Flush any remaining events before shutting down
    if event_counter > 0:
        print(f"\n💾 Flushing {event_counter} remaining events before shutdown...")
        flush_all_buffers()

    consumer.close()
    print("✅ Lake Writer shut down cleanly.")
