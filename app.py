from flask import Flask, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from database import db
from models import User, Product, Order, Payment
from kafka_producer import send_event
from datetime import datetime, timezone
import stripe
import os
import uuid

# ── Load environment variables ────────────────────────────────────
from dotenv import load_dotenv
load_dotenv()

# ── Create Flask App ──────────────────────────────────────────────
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key')
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# ── Initialize Database ───────────────────────────────────────────
db.init_app(app)

# ── Initialize Flask-Login ────────────────────────────────────────
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access this page.'


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


# ── Initialize Stripe ────────────────────────────────────────────
stripe.api_key = os.getenv('STRIPE_SECRET_KEY')

# ── Create tables on first run ────────────────────────────────────
with app.app_context():
    db.create_all()
    print("✅ Database tables created.")


# ══════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════

# ── Home Page (Product Catalog) ───────────────────────────────────
@app.route('/')
def home():
    products = Product.query.filter(Product.stock > 0).all()
    return render_template('home.html', products=products)


# ── Register ──────────────────────────────────────────────────────
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        name = request.form['name']
        email = request.form['email']
        password = request.form['password']

        # Check if email already exists
        existing = User.query.filter_by(email=email).first()
        if existing:
            flash('An account with that email already exists.', 'error')
            return redirect(url_for('register'))

        # Create new user
        user = User(
            name=name,
            email=email,
            password_hash=generate_password_hash(password)
        )
        db.session.add(user)
        db.session.commit()

        flash('Account created! Please log in.', 'success')
        return redirect(url_for('login'))

    return render_template('register.html')


# ── Login ─────────────────────────────────────────────────────────
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']

        user = User.query.filter_by(email=email).first()
        if user and check_password_hash(user.password_hash, password):
            login_user(user)
            flash(f'Welcome back, {user.name}!', 'success')
            return redirect(url_for('home'))
        else:
            flash('Invalid email or password.', 'error')

    return render_template('login.html')


# ── Logout ────────────────────────────────────────────────────────
@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out.', 'success')
    return redirect(url_for('home'))


# ── Single Product Page ───────────────────────────────────────────
@app.route('/product/<int:product_id>')
def product_detail(product_id):
    product = db.session.get(Product, product_id)
    if not product:
        flash('Product not found.', 'error')
        return redirect(url_for('home'))
    return render_template('product.html',
                           product=product,
                           stripe_key=os.getenv('STRIPE_PUBLISHABLE_KEY'))


# ── Stripe Checkout ───────────────────────────────────────────────
@app.route('/buy/<int:product_id>', methods=['POST'])
@login_required
def buy_product(product_id):
    product = db.session.get(Product, product_id)
    if not product or product.stock <= 0:
        flash('Product is out of stock.', 'error')
        return redirect(url_for('home'))

    # Create a pending order
    order_number = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    order = Order(
        order_number=order_number,
        user_id=current_user.id,
        product_id=product.id,
        quantity=1,
        total_price=product.price,
        status='pending'
    )
    db.session.add(order)
    db.session.commit()

    # Create Stripe checkout session
    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{
            'price_data': {
                'currency': 'usd',
                'product_data': {
                    'name': product.name,
                    'description': product.description or '',
                },
                'unit_amount': int(product.price * 100),  # Stripe uses cents
            },
            'quantity': 1,
        }],
        mode='payment',
        success_url=url_for('checkout_success', order_id=order.id, _external=True),
        cancel_url=url_for('product_detail', product_id=product.id, _external=True),
        metadata={
            'order_id': order.id,
            'order_number': order_number,
        }
    )

    # Save Stripe session ID to the order
    order.stripe_session_id = session.id
    db.session.commit()

    return redirect(session.url)


# ── Checkout Success ──────────────────────────────────────────────
@app.route('/checkout/success/<int:order_id>')
@login_required
def checkout_success(order_id):
    order = db.session.get(Order, order_id)
    if not order or order.user_id != current_user.id:
        flash('Order not found.', 'error')
        return redirect(url_for('home'))

    # Only process if not already processed
    if order.status == 'pending':
        # Update order status
        order.status = 'paid'
        db.session.commit()

        # Save payment record
        payment = Payment(
            order_id=order.id,
            amount=order.total_price,
            status='completed',
            stripe_payment_id=order.stripe_session_id
        )
        db.session.add(payment)
        db.session.commit()

        # ── TRIGGER THE KAFKA PIPELINE ────────────────────────
        # This is where the magic happens. One event kicks off
        # the entire chain of microservices.

        product = db.session.get(Product, order.product_id)

        # Event 1: Order placed
        send_event('order-placed', order.order_number, {
            'order_number': order.order_number,
            'customer_name': current_user.name,
            'customer_email': current_user.email,
            'item': product.name,
            'quantity': order.quantity,
            'total_price': order.total_price,
            'order_id': order.id,
            'product_id': product.id,
        })

        # Event 2: Payment received
        send_event('payment-received', order.order_number, {
            'order_number': order.order_number,
            'customer_name': current_user.name,
            'customer_email': current_user.email,
            'amount': order.total_price,
            'item': product.name,
            'payment_id': payment.id,
            'order_id': order.id,
        })

        print(f"🎉 Order {order.order_number} fully processed and events published!")

    return render_template('checkout_success.html', order=order)


# ── My Orders ─────────────────────────────────────────────────────
@app.route('/orders')
@login_required
def my_orders():
    orders = Order.query.filter_by(user_id=current_user.id)\
                        .order_by(Order.created_at.desc()).all()
    return render_template('orders.html', orders=orders)


# ── Order Detail ──────────────────────────────────────────────────
@app.route('/orders/<int:order_id>')
@login_required
def order_detail(order_id):
    order = db.session.get(Order, order_id)
    if not order or order.user_id != current_user.id:
        flash('Order not found.', 'error')
        return redirect(url_for('my_orders'))
    return render_template('order_detail.html', order=order)


# ══════════════════════════════════════════════════════════════════
# RUN THE APP
# ══════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    app.run(debug=True, port=5000)