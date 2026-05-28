from confluent_kafka import Consumer
import json
import signal
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

# Flask app context needed to access the database
from app import app
from database import db
from models import Product

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stock-service',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe(['order-placed'])

print("📦 Stock Service is running!")
print("👂 Listening on topic: 'order-placed'")
print("   Press Ctrl+C to stop\n")

running = True

def stop(_sig, _frame):
    global running
    print("\n⚠️  Stopping Stock Service...")
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
        print(f"📦 Order {order['order_number']}: {order['item']}")

        # Update stock in PostgreSQL
        with app.app_context():
            product = db.session.get(Product, order['product_id'])
            if product and product.stock > 0:
                product.stock -= order['quantity']
                db.session.commit()
                print(f"  ✅ Stock updated: {product.name} → {product.stock} remaining")
            else:
                print(f"  ❌ Could not update stock for product {order['product_id']}")
        print()

finally:
    consumer.close()
    print("✅ Stock Service shut down cleanly.")
