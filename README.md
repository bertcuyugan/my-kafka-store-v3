# 🏪 My Kafka Store V3 — Event-Driven E-Commerce

A full-stack e-commerce application where every step of the buying process flows through Apache Kafka automatically — from order placement to delivery confirmation — with a data lake pipeline feeding a Databricks medallion model.

Built with Flask, Kafka, PostgreSQL, Stripe, Gmail, and Databricks.

---

## How It Works

A customer registers, browses products, and clicks "Buy Now". Stripe handles the payment. Once confirmed, a chain of Kafka events fires automatically — no manual triggering:

```
Customer clicks "Buy Now"
        │
        ▼
   Stripe Checkout (test mode)
        │
        ▼ payment confirmed
   Flask app publishes events
        │
        ├──► "order-placed" ──┬──► Email Service (sends confirmation email)
        │                     └──► Stock Service (updates inventory in DB)
        │
        └──► "payment-received" ──┬──► Invoice Service (emails invoice)
                                  └──► Shipping Service (waits 30s, then ships)
                                              │
                                              ▼
                                       "item-shipped" ──► Tracking Service
                                                          (emails tracking number)
                                                          (waits 60s, then delivers)
                                                                   │
                                                                   ▼
                                                          "item-delivered" ──► Delivery Service
                                                                               (emails delivery confirmation)
                                                                               (marks order as "delivered" in DB)

   ALL 4 TOPICS ──► Lake Writer Service ──► Parquet files ──► Databricks (Bronze → Silver → Gold)
```

The customer receives 4 real emails and can track their order status in the web app. Meanwhile, every event is captured as Parquet files for analytics in Databricks.

---

## Features

- **User registration & login** with password hashing
- **Product catalog** with images, prices, and stock tracking
- **Stripe checkout** (test mode — no real money)
- **4 Kafka topics** driving an automated event pipeline
- **7 independent microservices** each doing one job
- **Real Gmail emails** at every stage (confirmation, invoice, tracking, delivery)
- **Order history page** with a visual status timeline (pending → paid → shipped → delivered)
- **PostgreSQL database** storing users, products, orders, payments, and shipments
- **Data lake pipeline** writing all Kafka events to Parquet files
- **Databricks medallion model** (Bronze → Silver → Gold) for analytics

---

## Tech Stack

| Component | Technology |
|---|---|
| Frontend | Flask + Jinja2 + CSS |
| Backend | Python (Flask) |
| Database | PostgreSQL 16 (Docker) |
| Message Broker | Apache Kafka 3.9.2 (Docker, KRaft mode) |
| Payments | Stripe (test mode) |
| Emails | Gmail SMTP |
| Data Lake | Parquet files via fastparquet + pandas |
| Analytics | Databricks (PySpark, medallion architecture) |
| Containers | Docker Compose |

---

## Project Structure

```
my-kafka-store-v3/
├── docker-compose.yml          # Kafka + PostgreSQL
├── requirements.txt            # Python dependencies
├── .env                        # Secrets (not committed)
├── .gitignore
├── README.md
│
├── app.py                      # Flask web app (routes, auth, Stripe)
├── models.py                   # Database models
├── database.py                 # Database connection
├── kafka_producer.py           # Shared Kafka producer
├── seed_products.py            # Populates DB with sample products
│
├── services/
│   ├── email_service.py        # Sends confirmation email (order-placed)
│   ├── stock_service.py        # Updates inventory (order-placed)
│   ├── invoice_service.py      # Emails invoice (payment-received)
│   ├── shipping_service.py     # Ships item + publishes item-shipped
│   ├── tracking_service.py     # Emails tracking + publishes item-delivered
│   ├── delivery_service.py     # Marks delivered + emails confirmation
│   └── lake_writer.py          # Writes all Kafka events to Parquet files
│
├── data-lake/                  # Generated Parquet files (gitignored)
│   ├── order-placed/
│   ├── payment-received/
│   ├── item-shipped/
│   └── item-delivered/
│
├── templates/                  # HTML pages
│   ├── base.html
│   ├── home.html
│   ├── register.html
│   ├── login.html
│   ├── product.html
│   ├── checkout_success.html
│   ├── orders.html
│   └── order_detail.html
│
└── static/
    └── style.css
```

---

## Prerequisites

- **Python 3.8+** — [Download](https://www.python.org/downloads/)
- **Docker Desktop** — [Download](https://www.docker.com/products/docker-desktop/)
- **Stripe account** (free) — [Sign up](https://dashboard.stripe.com/register)
- **Gmail App Password** — [Generate one](https://myaccount.google.com/apppasswords) (requires 2-Step Verification)
- **Databricks Community Edition** (free, for the medallion model) — [Sign up](https://community.cloud.databricks.com/login.html)

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/your-username/my-kafka-store-v3.git
cd my-kafka-store-v3
```

### 2. Create virtual environment and install dependencies

```bash
# Windows:
python -m venv .venv
.venv\Scripts\activate

# Mac/Linux:
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Create your `.env` file

Create a file called `.env` in the project root (this file is gitignored):

```env
STRIPE_SECRET_KEY=sk_test_YOUR_KEY_HERE
STRIPE_PUBLISHABLE_KEY=pk_test_YOUR_KEY_HERE
GMAIL_ADDRESS=your.email@gmail.com
GMAIL_APP_PASSWORD=abcd efgh ijkl mnop
DATABASE_URL=postgresql+psycopg://kafkauser:kafkapass@localhost:5432/kafkastore
FLASK_SECRET_KEY=any-random-string-here
```

### 4. Start Kafka and PostgreSQL

Open Docker Desktop first (wait for the whale icon), then:

```bash
docker compose up -d
```

Verify both containers are running:

```bash
docker ps
```

You should see `broker` and `postgres` both with status `Up`.

### 5. Create Kafka topics

```bash
docker exec --workdir /opt/kafka/bin/ -it broker bash
```

Inside the container, run:

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-placed --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic payment-received --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic item-shipped --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic item-delivered --partitions 3 --replication-factor 1
exit
```

### 6. Seed the database with products

```bash
python seed_products.py
```

---

## Running the App

You need **8 terminal windows**. In each one, `cd` to the project folder and activate `.venv`.

### Start the 7 microservices (one per terminal):

```bash
# Terminal 1
python services/email_service.py

# Terminal 2
python services/stock_service.py

# Terminal 3
python services/invoice_service.py

# Terminal 4
python services/shipping_service.py

# Terminal 5
python services/tracking_service.py

# Terminal 6
python services/delivery_service.py

# Terminal 7 — Data Lake Writer
python services/lake_writer.py
```

### Start the Flask web app:

```bash
# Terminal 8
python app.py
```

### Open the store:

Go to **http://localhost:5000**

---

## Using the Store

1. **Register** an account (use a real email to receive the emails)
2. **Log in**
3. **Browse** the product catalog
4. **Click a product** → click **"Buy Now"**
5. **Pay with Stripe** using test card: `4242 4242 4242 4242`, any future expiry, any CVC
6. **Watch the terminals** — events cascade automatically through all 7 services
7. **Check your email** — you'll receive 4 emails (confirmation, invoice, tracking, delivery)
8. **View "My Orders"** — see the status timeline update from paid → shipped → delivered
9. **Check `data-lake/` folder** — Parquet files appear as events flow through Kafka

### Stripe Test Cards

| Card Number | Result |
|---|---|
| `4242 4242 4242 4242` | Successful payment |
| `4000 0000 0000 0002` | Card declined |
| `4000 0000 0000 3220` | Requires 3D Secure |

---

## Event Timeline

After a successful purchase, this is what happens automatically:

| Time | Event | Service |
|---|---|---|
| 0s | `order-placed` published | Flask app |
| 0s | Stock updated in database | Stock Service |
| 0s | Confirmation email sent | Email Service |
| 0s | Invoice emailed | Invoice Service |
| 0s | Event written to Parquet | Lake Writer |
| ~30s | Item shipped, tracking created | Shipping Service |
| ~30s | Tracking email sent | Tracking Service |
| ~30s | Shipping event written to Parquet | Lake Writer |
| ~90s | Item marked as delivered | Delivery Service |
| ~90s | Delivery confirmation email sent | Delivery Service |
| ~90s | Delivery event written to Parquet | Lake Writer |

---

## Kafka Topics

| Topic | Published by | Consumed by |
|---|---|---|
| `order-placed` | Flask app (on payment success) | Email Service, Stock Service, Lake Writer |
| `payment-received` | Flask app (on payment success) | Invoice Service, Shipping Service, Lake Writer |
| `item-shipped` | Shipping Service | Tracking Service, Lake Writer |
| `item-delivered` | Tracking Service | Delivery Service, Lake Writer |

---

## Data Lake Pipeline

The Lake Writer service listens to all 4 Kafka topics and writes every event as Parquet files:

```
data-lake/
├── order-placed/
│   └── 2026-05-28/
│       └── batch_143022_123456.parquet
├── payment-received/
│   └── 2026-05-28/
│       └── batch_143022_234567.parquet
├── item-shipped/
│   └── 2026-05-28/
│       └── batch_143052_345678.parquet
└── item-delivered/
    └── 2026-05-28/
        └── batch_143122_456789.parquet
```

Each Parquet file contains:

| Column | Description |
|---|---|
| `event_topic` | Which Kafka topic it came from |
| `event_key` | Message key (order number) |
| `event_partition` | Kafka partition number |
| `event_offset` | Position in partition |
| `event_timestamp` | When the lake writer ingested it |
| `event_data` | Full event payload as raw JSON |

### Verify Parquet files locally:

```bash
python -c "import pandas as pd; print(pd.read_parquet('data-lake/order-placed', engine='fastparquet').to_string())"
```

---

## Databricks Medallion Model

Upload the `data-lake/` folder to Databricks Community Edition, then build Bronze → Silver → Gold layers:

### Upload to Databricks

1. Go to **Workspace → Users → your email**
2. Create a folder called `data-lake` with subfolders for each topic
3. Upload Parquet files into the matching subfolders

### Medallion Architecture

```
Bronze (Raw)              Silver (Cleaned)           Gold (Business)
──────────────────        ──────────────────         ──────────────────
Raw Parquet as-is         Parsed JSON into           Aggregated tables:
from Kafka events         typed columns,             • Order Journey
                          deduplicated               • Revenue Summary
                                                     • Top Products
"Store everything"        "Clean everything"         • Customer Summary
                                                     • Delivery Performance
```

### Read Bronze layer in Databricks:

```python
base_path = "/Workspace/Users/your.email@gmail.com/data-lake"

bronze_orders = spark.read.parquet(f"{base_path}/order-placed/")
bronze_payments = spark.read.parquet(f"{base_path}/payment-received/")
bronze_shipments = spark.read.parquet(f"{base_path}/item-shipped/")
bronze_deliveries = spark.read.parquet(f"{base_path}/item-delivered/")
```

See the full Databricks notebook code in the [Data Lake Guide](kafka-datalake-guide.md).

---

## Database Tables

| Table | What it stores |
|---|---|
| `users` | Customer accounts (name, email, hashed password) |
| `products` | Store catalog (name, price, stock, image) |
| `orders` | Purchase records (order number, status, timestamps) |
| `payments` | Payment records (amount, Stripe session ID) |
| `shipments` | Shipping records (tracking number, shipped/delivered dates) |

---

## Shutting Down

1. Press `Ctrl+C` in each service terminal (wait for "shut down cleanly")
2. Press `Ctrl+C` in the Flask terminal
3. Run: `docker compose down`

---

## Useful Commands

```bash
# Start/stop infrastructure
docker compose up -d
docker compose down

# Check containers
docker ps
docker logs broker
docker logs postgres

# Kafka CLI
docker exec --workdir /opt/kafka/bin/ -it broker bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order-placed --from-beginning

# Database
docker exec -it postgres psql -U kafkauser -d kafkastore
SELECT * FROM orders;
SELECT * FROM products;
\q

# Verify Parquet files
python -c "import pandas as pd; print(pd.read_parquet('data-lake/order-placed', engine='fastparquet').to_string())"
```

---

## Common Issues

| Problem | Fix |
|---|---|
| `Connection refused :9092` | Run `docker compose up -d` and wait for containers to start |
| `Connection refused :5432` | Same — PostgreSQL needs a moment to initialize |
| `SMTP authentication error` | Regenerate Gmail App Password at myaccount.google.com/apppasswords |
| `Stripe invalid API key` | Check your `.env` keys match dashboard.stripe.com/test/apikeys |
| `No products on home page` | Run `python seed_products.py` |
| `ModuleNotFoundError` | Activate `.venv` first |
| `UNKNOWN_TOPIC_OR_PART` | Create topics manually (see Setup step 5) |
| Service shows wrong topic | Ensure `email_service.py` has `if __name__ == '__main__':` guard around its consumer code |
| Emails not arriving | Check spam folder; verify Gmail App Password in `.env` |
| `psycopg2 not found` | Use `postgresql+psycopg://` in DATABASE_URL (not `postgresql://`) |
| `psycopg2-binary` won't install on Windows | Use `psycopg[binary]` instead (already in requirements.txt) |
| `pyarrow` won't install (cmake error) | Use `fastparquet` + `pandas` instead (already in requirements.txt) |
| Parquet files not appearing in `data-lake/` | Make sure `lake_writer.py` is running; wait 30s for the buffer to flush |
| Databricks can't find Parquet files | Path is `/Workspace/Users/your-email/data-lake/` not `/FileStore/tables/` |

---
