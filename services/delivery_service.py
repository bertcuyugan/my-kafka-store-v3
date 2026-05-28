from confluent_kafka import Consumer
import json
import signal
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from app import app
from database import db
from models import Order, Shipment
from services.email_service import send_email

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'delivery-service',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe(['item-delivered'])

print("✅ Delivery Service is running!")
print("👂 Listening on topic: 'item-delivered'")
print("   Press Ctrl+C to stop\n")

running = True

def stop(_sig, _frame):
    global running
    print("\n⚠️  Stopping Delivery Service...")
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

        delivery = json.loads(raw_value.decode('utf-8'))
        print(f"📬 Item delivered: {delivery['order_number']}")

        # Update order and shipment status in database
        with app.app_context():
            order = Order.query.filter_by(order_number=delivery['order_number']).first()
            if order:
                order.status = 'delivered'
                if order.shipment:
                    order.shipment[0].status = 'delivered'
                    order.shipment[0].delivered_at = datetime.now(timezone.utc)
                db.session.commit()
                print(f"  ✅ Order {delivery['order_number']} marked as DELIVERED")

        # Send delivery confirmation email
        send_email(
            to_email=delivery['customer_email'],
            subject=f"Order Delivered — {delivery['order_number']}",
            body_html=f"""
            <h2>Your order has been delivered! ✅</h2>
            <p>Hi {delivery['customer_name']},</p>
            <p>Your order <strong>{delivery['order_number']}</strong> has been delivered successfully.</p>
            <table style="border-collapse: collapse; margin: 20px 0;">
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Item</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{delivery['item']}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Tracking</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{delivery['tracking_number']}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Status</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">✅ Delivered</td>
                </tr>
            </table>
            <p>Thank you for shopping with Kafka Store!</p>
            <p>— Kafka Store Team</p>
            """
        )
        print()

finally:
    consumer.close()
    print("✅ Delivery Service shut down cleanly.")