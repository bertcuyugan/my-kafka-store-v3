from app import app
from database import db
from models import Product

products = [
    {
        "name": "Classic Blue Shirt",
        "description": "A timeless blue cotton shirt perfect for any occasion. Soft, breathable fabric with a modern slim fit.",
        "price": 29.99,
        "stock": 25,
        "image_url": "https://images.unsplash.com/photo-1596755094514-f87e34085b2c?w=400",
        "category": "Shirts"
    },
    {
        "name": "White Canvas Shoes",
        "description": "Clean, minimalist white sneakers. Lightweight canvas upper with a cushioned insole for all-day comfort.",
        "price": 79.99,
        "stock": 15,
        "image_url": "https://images.unsplash.com/photo-1525966222134-fcfa99b8ae77?w=400",
        "category": "Shoes"
    },
    {
        "name": "Gold Chain Necklace",
        "description": "Elegant 18K gold-plated chain necklace. Hypoallergenic and tarnish-resistant. Length: 18 inches.",
        "price": 49.99,
        "stock": 30,
        "image_url": "https://images.unsplash.com/photo-1599643478518-a784e5dc4c8f?w=400",
        "category": "Jewelry"
    },
    {
        "name": "Leather Crossbody Bag",
        "description": "Handcrafted genuine leather bag with adjustable strap. Multiple compartments for everyday essentials.",
        "price": 99.99,
        "stock": 10,
        "image_url": "https://images.unsplash.com/photo-1548036328-c9fa89d128fa?w=400",
        "category": "Bags"
    },
    {
        "name": "Red Flannel Shirt",
        "description": "Cozy flannel shirt in classic red plaid. Brushed cotton for extra softness. Relaxed fit.",
        "price": 34.99,
        "stock": 20,
        "image_url": "https://images.unsplash.com/photo-1589310243389-96a5483213a8?w=400",
        "category": "Shirts"
    },
    {
        "name": "Black Running Shoes",
        "description": "Lightweight performance running shoes with responsive cushioning and breathable mesh upper.",
        "price": 119.99,
        "stock": 12,
        "image_url": "https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=400",
        "category": "Shoes"
    },
    {
        "name": "Silver Hoop Earrings",
        "description": "Sterling silver hoop earrings. Classic medium size, perfect for everyday wear. Comes in a gift box.",
        "price": 24.99,
        "stock": 40,
        "image_url": "https://images.unsplash.com/photo-1630019852942-f89202989a59?w=400",
        "category": "Jewelry"
    },
    {
        "name": "Canvas Tote Bag",
        "description": "Heavy-duty canvas tote with reinforced handles. Spacious interior with an inner pocket. Machine washable.",
        "price": 19.99,
        "stock": 50,
        "image_url": "https://images.unsplash.com/photo-1544816155-12df9643f363?w=400",
        "category": "Bags"
    },
]

with app.app_context():
    # Only seed if the table is empty
    if Product.query.count() == 0:
        for p in products:
            product = Product(**p)
            db.session.add(product)
        db.session.commit()
        print(f"✅ Added {len(products)} products to the database!")
    else:
        print(f"ℹ️  Products already exist ({Product.query.count()} found). Skipping seed.")