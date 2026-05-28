from database import db
from flask_login import UserMixin
from datetime import datetime, timezone


class User(UserMixin, db.Model):
    """A customer who registers on our store."""
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # A user can have many orders
    orders = db.relationship('Order', backref='user', lazy=True)


class Product(db.Model):
    """An item for sale in our store."""
    __tablename__ = 'products'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    price = db.Column(db.Float, nullable=False)           # in dollars
    stock = db.Column(db.Integer, nullable=False, default=0)
    image_url = db.Column(db.String(500))
    category = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Order(db.Model):
    """A customer's purchase."""
    __tablename__ = 'orders'

    id = db.Column(db.Integer, primary_key=True)
    order_number = db.Column(db.String(50), unique=True, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=1)
    total_price = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(50), default='pending')
    # Status flow: pending → paid → shipped → delivered
    stripe_session_id = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    product = db.relationship('Product', backref='orders')


class Payment(db.Model):
    """Payment record for an order."""
    __tablename__ = 'payments'

    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(50), default='completed')
    stripe_payment_id = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    order = db.relationship('Order', backref='payment')


class Shipment(db.Model):
    """Shipping record for an order."""
    __tablename__ = 'shipments'

    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'), nullable=False)
    tracking_number = db.Column(db.String(100))
    status = db.Column(db.String(50), default='processing')
    # Status flow: processing → shipped → delivered
    shipped_at = db.Column(db.DateTime)
    delivered_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    order = db.relationship('Order', backref='shipment')