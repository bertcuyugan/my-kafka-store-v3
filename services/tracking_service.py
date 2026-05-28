from confluent_kafka import Consumer
import json
import signal
import time
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from services.email_service import send_email
from kafka_producer import send_event

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'tracking-service',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe(['item-shipped'])

print("🔍 Tracking Service is running!")
print("👂 Listening on topic: 'item-shipped'")
print("   Press Ctrl+C to stop\n")

running = True

def stop(_sig, _frame):
    global running
    print("\n⚠️  Stopping Tracking Service...")
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

        shipment = json.loads(raw_value.decode('utf-8'))
        print(f"📬 Item shipped: {shipment['order_number']} → {shipment['tracking_number']}")

        # Send tracking email
        send_email(
            to_email=shipment['customer_email'],
            subject=f"Your Order Has Shipped — {shipment['order_number']}",
            body_html=f"""
            <h2>Your order is on its way! 🚚</h2>
            <p>Hi {shipment['customer_name']},</p>
            <p>Great news! Your order <strong>{shipment['order_number']}</strong> has been shipped.</p>
            <table style="border-collapse: collapse; margin: 20px 0;">
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Item</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{shipment['item']}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Tracking Number</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{shipment['tracking_number']}</td>
                </tr>
            </table>
            <p>We'll notify you when it's delivered.</p>
            <p>— Kafka Store Team</p>
            """
        )

        # Simulate delivery time (60 seconds)
        print(f"  ⏳ Simulating delivery (60 seconds)...")
        for i in range(60, 0, -15):
            if not running:
                break
            print(f"  ⏳ Delivery in {i} seconds...")
            time.sleep(15)

        if not running:
            break

        # Publish item-delivered event → triggers Delivery Service
        send_event('item-delivered', shipment['order_number'], {
            'order_number': shipment['order_number'],
            'customer_name': shipment['customer_name'],
            'customer_email': shipment['customer_email'],
            'item': shipment['item'],
            'tracking_number': shipment['tracking_number'],
            'order_id': shipment['order_id'],
        })
        print()

finally:
    consumer.close()
    print("✅ Tracking Service shut down cleanly.")