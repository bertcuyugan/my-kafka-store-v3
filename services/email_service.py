from confluent_kafka import Consumer
import json
import signal
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

GMAIL_ADDRESS = os.getenv('GMAIL_ADDRESS')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')


def send_email(to_email, subject, body_html):
    """Send an email using Gmail SMTP."""
    msg = MIMEMultipart('alternative')
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body_html, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        print(f"  📧 Email sent to {to_email}")
    except Exception as e:
        print(f"  ❌ Failed to send email: {e}")


# ── Everything below only runs when you execute this file directly ──
# ── NOT when another file imports send_email from here ──────────────
if __name__ == '__main__':
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'email-service',
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe(['order-placed'])

    print("📧 Email Service is running!")
    print("👂 Listening on topic: 'order-placed'")
    print("   Press Ctrl+C to stop\n")

    running = True

    def stop(_sig, _frame):
        global running
        print("\n⚠️  Stopping Email Service...")
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

            order = json.loads(raw_value.decode('utf-8'))
            print(f"📨 New order: {order['order_number']}")

            send_email(
                to_email=order['customer_email'],
                subject=f"Order Confirmed — {order['order_number']}",
                body_html=f"""
                <h2>Thank you for your order, {order['customer_name']}!</h2>
                <p>Your order <strong>{order['order_number']}</strong> has been confirmed.</p>
                <table style="border-collapse: collapse; margin: 20px 0;">
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Item</strong></td>
                        <td style="padding: 8px; border: 1px solid #ddd;">{order['item']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Quantity</strong></td>
                        <td style="padding: 8px; border: 1px solid #ddd;">{order['quantity']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total</strong></td>
                        <td style="padding: 8px; border: 1px solid #ddd;">${order['total_price']:.2f}</td>
                    </tr>
                </table>
                <p>We'll send you another email when your item ships.</p>
                <p>— Kafka Store Team</p>
                """
            )
            print()

    finally:
        consumer.close()
        print("✅ Email Service shut down cleanly.")