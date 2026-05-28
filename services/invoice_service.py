from confluent_kafka import Consumer
import json
import signal
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from services.email_service import send_email

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'invoice-service',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe(['payment-received'])

print("🧾 Invoice Service is running!")
print("👂 Listening on topic: 'payment-received'")
print("   Press Ctrl+C to stop\n")

running = True

def stop(_sig, _frame):
    global running
    print("\n⚠️  Stopping Invoice Service...")
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
        print(f"🧾 Payment for {payment['order_number']}: ${payment['amount']:.2f}")

        # Send invoice email
        send_email(
            to_email=payment['customer_email'],
            subject=f"Invoice — {payment['order_number']}",
            body_html=f"""
            <h2>Invoice</h2>
            <p>Hi {payment['customer_name']},</p>
            <p>Here is your invoice for order <strong>{payment['order_number']}</strong>.</p>
            <table style="border-collapse: collapse; margin: 20px 0; width: 100%;">
                <tr style="background: #f5f5f5;">
                    <td style="padding: 10px; border: 1px solid #ddd;"><strong>Item</strong></td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{payment['item']}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;"><strong>Amount Paid</strong></td>
                    <td style="padding: 10px; border: 1px solid #ddd;">${payment['amount']:.2f}</td>
                </tr>
                <tr style="background: #f5f5f5;">
                    <td style="padding: 10px; border: 1px solid #ddd;"><strong>Status</strong></td>
                    <td style="padding: 10px; border: 1px solid #ddd;">✅ Paid</td>
                </tr>
            </table>
            <p>Thank you for shopping with us!</p>
            <p>— Kafka Store Team</p>
            """
        )
        print(f"  ✅ Invoice emailed to {payment['customer_email']}")
        print()

finally:
    consumer.close()
    print("✅ Invoice Service shut down cleanly.")