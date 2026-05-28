from confluent_kafka import Consumer
import json
import signal
import time
import uuid
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from app import app
from database import db
from models import Order, Shipment
from kafka_producer import send_event

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'shipping-service',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe(['payment-received'])

print("🚚 Shipping Service is running!")
print("👂 Listening on topic: 'payment-received'")
print("   Press Ctrl+C to stop\n")

running = True

def stop(_sig, _frame):
    global running
    print("\n⚠️  Stopping Shipping Service...")
    running = False

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

try:
    while running:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            print(f"❌ Error: {message.error()}")
            continue

        raw_value = message.value()
        if raw_value is None:
            continue

        payment = json.loads(raw_value.decode('utf-8'))
        order_number = payment['order_number']

        print(f"🚚 Preparing shipment for {order_number}...")
        print(f"   ⏳ Packing item (waiting 30 seconds to simulate processing)...")

        # Simulate warehouse processing time
        for i in range(30, 0, -10):
            if not running:
                break
            print(f"   ⏳ Shipping in {i} seconds...")
            time.sleep(10)

        if not running:
            break

        tracking_number = f"TRK-{uuid.uuid4().hex[:8].upper()}"

        # Save shipment to database and update order status
        with app.app_context():
            order = Order.query.filter_by(order_number=order_number).first()
            if order:
                shipment = Shipment(
                    order_id=order.id,
                    tracking_number=tracking_number,
                    status='shipped',
                    shipped_at=datetime.now(timezone.utc)
                )
                db.session.add(shipment)
                order.status = 'shipped'
                db.session.commit()
                print(f"  ✅ Shipment saved: {tracking_number}")

        # Publish item-shipped event → triggers Tracking Service
        send_event('item-shipped', order_number, {
            'order_number': order_number,
            'customer_name': payment['customer_name'],
            'customer_email': payment['customer_email'],
            'item': payment['item'],
            'tracking_number': tracking_number,
            'order_id': payment['order_id'],
        })
        print()

finally:
    consumer.close()
    print("✅ Shipping Service shut down cleanly.")