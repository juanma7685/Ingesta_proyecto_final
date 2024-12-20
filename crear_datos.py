
import numpy as np
import pandas as pd
import random
import uuid
from datetime import datetime, timedelta

# Funciones auxiliares
def generate_uuid():
    return str(uuid.uuid4())

def random_date(start, end):
    """Genera una fecha aleatoria entre start y end."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(random_days)

# Parámetros de generación
NUM_USERS = 100
NUM_ADDRESSES = 80
NUM_PRODUCTS = 50
NUM_PROMOS = 20
NUM_ORDERS = 500
NUM_EVENTS = 2000
NUM_BUDGET = 50

# Generar fechas
today = datetime.today()

# Direcciones (addresses) con claves compuestas
countries_with_states = {
    "United States": ["California", "Florida", "Texas", "New York", "Illinois"],
    "Spain": ["Madrid", "Barcelona", "Valencia", "Seville"],
    "Canada": ["Ontario", "Quebec", "British Columbia", "Alberta", "Nova Scotia"]
}

used_address_combinations = set()

def generate_unique_address_combination():
    while True:
        zipcode = random.randint(10000, 99999)
        country = random.choice(list(countries_with_states.keys()))
        state = random.choice(countries_with_states[country])
        address = f"{random.randint(1, 9999)} {random.choice(['Main St', 'Broadway', 'Park Ave'])}"
        combination = (zipcode, country, address, state)
        if combination not in used_address_combinations:
            used_address_combinations.add(combination)
            return combination

addresses_data = [generate_unique_address_combination() for _ in range(NUM_ADDRESSES)]
addresses = pd.DataFrame(addresses_data, columns=["ZIPCODE", "COUNTRY", "ADDRESS", "STATE"])

# Usuarios (users) con clave primaria EMAIL
used_emails = set()
used_phone_numbers = set()

def generate_unique_email(first_name, last_name):
    while True:
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 10000)}@example.com"
        if email not in used_emails:
            used_emails.add(email)
            return email

def generate_unique_phone():
    while True:
        phone = f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(100, 999)}"
        if phone not in used_phone_numbers:
            used_phone_numbers.add(phone)
            return phone

first_names = ["John", "Mary", "Robert", "Linda", "James", "Patricia", "Michael", "Barbara", "David", "Elizabeth"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]

# Asignar direcciones válidas a los usuarios
users = pd.DataFrame({
    "UPDATED_AT": [random_date(today - timedelta(days=1000), today).isoformat() for _ in range(NUM_USERS)],
    "LAST_NAME": [random.choice(last_names) for _ in range(NUM_USERS)],
    "CREATED_AT": [random_date(today - timedelta(days=1500), today - timedelta(days=1000)).isoformat() for _ in range(NUM_USERS)],
    "PHONE_NUMBER": [generate_unique_phone() for _ in range(NUM_USERS)],
    "FIRST_NAME": [random.choice(first_names) for _ in range(NUM_USERS)],
})

# Asignar direcciones válidas aleatorias a los usuarios
valid_addresses = addresses.sample(n=NUM_USERS, replace=True).reset_index(drop=True)
users = pd.concat([users, valid_addresses], axis=1)

# Generar emails únicos
users["EMAIL"] = users.apply(lambda row: generate_unique_email(row["FIRST_NAME"], row["LAST_NAME"]), axis=1)


# Productos (products) con clave compuesta NAME + CATEGORY
categories_with_products = {
    "Electronics": ["Smartphone", "Laptop", "Tablet", "Headphones", "Smartwatch", "Camera", "Drone", "TV", "Speaker", "Monitor"],
    "Clothing": ["Shirt", "Jeans", "Jacket", "Shoes", "Sweater", "T-shirt", "Skirt", "Dress", "Coat", "Socks"],
    "Books": ["Fiction", "Non-fiction", "Comics", "Magazines", "Biography", "Science", "History", "Mystery", "Fantasy", "Horror"],
    "Weapons": ["AK-47", "Pistol", "Rifle", "Knife", "Shotgun", "Sniper", "Crossbow", "Taser", "Pepper Spray", "Baton"],
    "Furniture": ["Chair", "Table", "Sofa", "Bed", "Desk", "Wardrobe", "Shelf", "Dresser", "Bench", "Cabinet"],
    "Toys": ["Action Figure", "Doll", "Board Game", "Puzzle", "LEGO", "RC Car", "Drone Toy", "Yo-yo", "Stuffed Animal", "Kite"],
    "Food": ["Apple", "Banana", "Bread", "Cheese", "Chocolate", "Milk", "Pizza", "Burger", "Pasta", "Steak"],
    "Automotive": ["Tire", "Engine Oil", "Car Battery", "Car Seat", "GPS", "Wipers", "Headlight", "Mats", "Air Freshener", "Jack"],
    "Sports": ["Basketball", "Soccer Ball", "Tennis Racket", "Baseball Bat", "Helmet", "Running Shoes", "Jersey", "Golf Clubs", "Dumbbells", "Yoga Mat"],
    "Healthcare": ["Bandage", "Thermometer", "Gloves", "Mask", "Sanitizer", "Blood Pressure Monitor", "First Aid Kit", "Pulse Oximeter", "Wheelchair", "Crutches"]
}


# Limitar productos para garantizar combinaciones únicas de NAME + CATEGORY
used_product_combinations = set()
products_data = []

for _ in range(NUM_PRODUCTS * 2):  # Generar un pool mayor para garantizar unicidad
    category = random.choice(list(categories_with_products.keys()))
    name = random.choice(categories_with_products[category])
    if (name, category) not in used_product_combinations:
        used_product_combinations.add((name, category))
        products_data.append({
            "PRICE": round(random.uniform(5.0, 2000.0), 2),
            "NAME": name,
            "CATEGORY": category,
            "INVENTORY": random.randint(30, 100)
        })
        if len(products_data) >= NUM_PRODUCTS:  # Limitar a NUM_PRODUCTS
            break

products = pd.DataFrame(products_data)

# Limitar promociones para garantizar claves únicas en PROMO_ID
used_promo_ids = set()
promos_data = []

for _ in range(NUM_PROMOS * 2):  # Generar un pool mayor para garantizar unicidad
    promo_name = f"Promo{random.randint(5, 50)}EUR"
    if promo_name not in used_promo_ids:
        used_promo_ids.add(promo_name)
        promos_data.append({
            "PROMO_ID": promo_name,
            "DISCOUNT": int(promo_name.replace("Promo", "").replace("EUR", "")),
            "STATUS": random.choice(["active", "inactive"])
        })
        if len(promos_data) >= NUM_PROMOS:  # Limitar a NUM_PROMOS
            break

promos = pd.DataFrame(promos_data)
# Asignar direcciones válidas a los pedidos
orders = []
for _ in range(NUM_ORDERS):
    created_at = random_date(today - timedelta(days=500), today)
    estimated_delivery_at = random_date(created_at, today)
    delivered_at = random_date(estimated_delivery_at, today) if random.random() > 0.3 else None
    status = "delivered" if delivered_at else random.choice(["preparing", "shipped"])
    shipping_service = random.choice(["ups", "usps", "fedex"]) if status != "preparing" else None
    shipping_cost = round(random.uniform(1, 3) if status == "preparing" else random.uniform(2, 10), 2)
    promo_id = random.choice(promos["PROMO_ID"]) if random.random() > 0.3 else None
    order_cost = round(random.uniform(50.0, 600.0), 2)
    order_total = order_cost + shipping_cost

    # Seleccionar dirección válida aleatoria
    valid_address = addresses.sample(n=1).iloc[0]

    orders.append({
        "ORDER_ID": generate_uuid(),
        "SHIPPING_SERVICE": shipping_service,
        "SHIPPING_COST": shipping_cost,
        "ZIPCODE": valid_address["ZIPCODE"],
        "COUNTRY": valid_address["COUNTRY"],
        "ADDRESS": valid_address["ADDRESS"],
        "STATE": valid_address["STATE"],
        "EMAIL": random.choice(users["EMAIL"]),
        "CREATED_AT": created_at.isoformat(),
        "PROMO_ID": promo_id,
        "ESTIMATED_DELIVERY_AT": estimated_delivery_at.isoformat(),
        "ORDER_COST": order_cost,
        "ORDER_TOTAL": order_total,
        "DELIVERED_AT": delivered_at.isoformat() if delivered_at else None,
        "TRACKING_ID": generate_uuid(),
        "STATUS": status
    })

orders = pd.DataFrame(orders[:NUM_ORDERS])

# Recalcular el número total de pedidos por usuario
user_order_counts = orders["EMAIL"].value_counts().to_dict()
users["TOTAL_ORDERS"] = users["EMAIL"].map(user_order_counts).fillna(0).astype(int)

# Crear order_items respetando la estructura existente
order_items = []
for order_id in orders["ORDER_ID"]:
    num_items = random.randint(1, 5)  # Cada pedido tendrá entre 1 y 5 productos
    selected_products = random.sample(list(zip(products["NAME"], products["CATEGORY"])), k=num_items)
    for name, category in selected_products:
        quantity = random.randint(1, 10)  # Cantidad aleatoria por producto
        order_items.append({
            "ORDER_ID": order_id,
            "NAME": name,
            "CATEGORY": category,
            "QUANTITY": quantity
        })

# Convertir order_items en DataFrame
order_items = pd.DataFrame(order_items)

# Recalcular order_cost con dos decimales
def calculate_order_cost(order_id):
    items = order_items[order_items["ORDER_ID"] == order_id]
    total_cost = 0
    for _, item in items.iterrows():
        price = products.loc[
            (products["NAME"] == item["NAME"]) & (products["CATEGORY"] == item["CATEGORY"]),
            "PRICE"
        ].values[0]
        total_cost += price * item["QUANTITY"]
    return round(total_cost, 2)

orders["ORDER_COST"] = orders["ORDER_ID"].apply(calculate_order_cost)

# Recalcular order_total con dos decimales
orders["ORDER_TOTAL"] = orders.apply(
    lambda row: round(row["ORDER_COST"] + row["SHIPPING_COST"], 2), axis=1
)


# Mapear categorías a dominios
category_domains = {
    "Electronics": "https://electronics-shop.com/",
    "Clothing": "https://clothing-store.com/",
    "Books": "https://books-world.com/",
    "Weapons": "https://weapons-gear.com/",
    "Furniture": "https://furniture-home.com/",
    "Toys": "https://toys-paradise.com/",
    "Food": "https://food-marketplace.com/",
    "Automotive": "https://auto-parts-store.com/",
    "Sports": "https://sports-equipment.com/",
    "Healthcare": "https://healthcare-supplies.com/"
}

# Eventos (events) referenciando dominios por categoría
events = []
for _ in range(NUM_EVENTS):
    event_type = random.choice(["add_to_cart", "page_view", "checkout", "package_shipped"])
    product_name, product_category = (None, None)
    domain = "https://general-store.com/"
    if event_type not in ["checkout", "package_shipped"]:
        product_name, product_category = random.choice(list(zip(products["NAME"], products["CATEGORY"])))
        domain = category_domains.get(product_category, domain)
    order_id = random.choice(orders["ORDER_ID"]) if event_type in ["checkout", "package_shipped"] else None

    events.append({
        "EVENT_ID": generate_uuid(),
        "PAGE_URL": f"{domain}{generate_uuid()}",
        "EVENT_TYPE": event_type,
        "EMAIL": random.choice(users["EMAIL"]),
        "NAME": product_name,
        "CATEGORY": product_category,
        "SESSION_ID": generate_uuid(),
        "CREATED_AT": random_date(today - timedelta(days=500), today).isoformat(),
        "ORDER_ID": order_id
    })
events = pd.DataFrame(events[:NUM_EVENTS])

# Presupuesto (budget) referenciando combinación NAME + CATEGORY
budget = []
for _ in range(NUM_BUDGET):
    product_name, product_category = random.choice(list(zip(products["NAME"], products["CATEGORY"])))
    month = random_date(today - timedelta(days=730), today).date()
    quantity = random.randint(10, 100)
    budget.append({
        "_ROW": len(budget) + 1,
        "QUANTITY": quantity,
        "MONTH": month,
        "NAME": product_name,
        "CATEGORY": product_category
    })
budget = pd.DataFrame(budget[:NUM_BUDGET])

# Tabla de reseñas (reviews)
reviews = []
review_comments = {
    1: "Terrible quality, do not buy.",
    2: "Not as expected, very disappointing.",
    3: "It's okay, could be better.",
    4: "Good value for money, happy with the purchase.",
    5: "Excellent product, highly recommend!"
}

# Solo la mitad de los usuarios escriben reseñas
users_who_review = random.sample(list(users["EMAIL"]), k=NUM_USERS // 2)

for email in users_who_review:
    num_reviews = random.randint(1, 3)  # Cada usuario puede dejar de 1 a 3 reseñas
    for _ in range(num_reviews):
        product_name, product_category = random.choice(list(zip(products["NAME"], products["CATEGORY"])))
        rating = random.randint(1, 5)
        reviews.append({
            "REVIEW_ID": generate_uuid(),
            "EMAIL": email,
            "NAME": product_name,
            "CATEGORY": product_category,
            "RATING": rating,
            "COMMENTS": review_comments[rating],
            "CREATED_AT": random_date(today - timedelta(days=500), today).isoformat()
        })

reviews = pd.DataFrame(reviews)

# Tabla de devoluciones (returns)
returns = []
for _ in range(int(NUM_ORDERS * 0.1)):  # Supongamos que el 10% de los pedidos son devueltos
    order_id = random.choice(orders["ORDER_ID"])
    email = orders.loc[orders["ORDER_ID"] == order_id, "EMAIL"].values[0]
    returns.append({
        "RETURN_ID": generate_uuid(),
        "ORDER_ID": order_id,
        "EMAIL": email,
        "REASON": random.choice([
            "Defective item", "Wrong item sent", "No longer needed", 
            "Found a better price", "Other"
        ]),
        "RETURN_DATE": random_date(today - timedelta(days=500), today).isoformat()
    })
returns = pd.DataFrame(returns)

# Guardar datos en CSV
addresses.to_csv("addresses.csv", index=False)
users.to_csv("users.csv", index=False)
products.to_csv("products.csv", index=False)
promos.to_csv("promos.csv", index=False)
orders.to_csv("orders.csv", index=False)
order_items.to_csv("order_items.csv", index=False)
events.to_csv("events.csv", index=False)
budget.to_csv("budget.csv", index=False)
reviews.to_csv("reviews.csv", index=False)
returns.to_csv("returns.csv", index=False)

print("Datos generados y guardados en CSV.")
