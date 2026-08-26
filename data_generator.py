import os
import json
import random
from datetime import datetime

# Configuration
DATA_DIR = r'C:\Users\DELL\Desktop\snowmart_github_repo\04_mock_data'
os.makedirs(DATA_DIR, exist_ok=True)

CITIES = ['Cairo', 'Alexandria', 'Giza', 'Mansoura', 'Luxor', 'Aswan', 'Tanta', 'Port Said']
ITEMS = [('Laptop', 1200.00), ('Mouse', 25.00), ('Keyboard', 45.00), ('Monitor', 300.00), ('Headphones', 150.00), ('Tablet', 600.00), ('Cover', 30.00)]
FIRST_NAMES = ['Ahmed', 'Mohamed', 'Mahmoud', 'Sarah', 'Mona', 'Nour', 'Omar', 'Youssef', 'Salma', 'Hala']
LAST_NAMES = ['Ali', 'Hassan', 'Ibrahim', 'Tarek', 'Kamal', 'Mostafa', 'Fathy', 'Saad']

def generate_customers(start_id, count, date_str):
    customers = []
    for i in range(count):
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        customers.append({
            "customer_id": start_id + i,
            "name": f"{fname} {lname}",
            "email": f"{fname.lower()}.{lname.lower()}{start_id+i}@email.com",
            "location": {"city": random.choice(CITIES), "country": "Egypt"},
            "updated_at": f"{date_str} {random.randint(8,12):02d}:{random.randint(0,59):02d}:00"
        })
    return customers

def generate_orders(start_order_id, count, customer_ids, date_str):
    orders = []
    for i in range(count):
        num_items = random.randint(1, 3)
        items = []
        for _ in range(num_items):
            item_name, price = random.choice(ITEMS)
            items.append({"item_name": item_name, "price": price, "qty": random.randint(1, 2)})
        
        orders.append({
            "order_id": start_order_id + i,
            "customer_id": random.choice(customer_ids),
            "items": items,
            "order_date": f"{date_str} {random.randint(10,22):02d}:{random.randint(0,59):02d}:00"
        })
    return orders

print("Generating Day 1 Data...")
day1_customers = generate_customers(100, 200, "2026-08-20")
day1_cust_ids = [c["customer_id"] for c in day1_customers]
day1_orders = generate_orders(5000, 500, day1_cust_ids, "2026-08-20")

with open(os.path.join(DATA_DIR, 'customers_day1.json'), 'w', encoding='utf-8') as f:
    for c in day1_customers: f.write(json.dumps(c) + '\n')

with open(os.path.join(DATA_DIR, 'orders_day1.json'), 'w', encoding='utf-8') as f:
    for o in day1_orders: f.write(json.dumps(o) + '\n')

print("Generating Day 2 Data (CDC Simulation)...")
# Day 2: 20 existing customers move to a new city (SCD2 trigger)
day2_customers = []
for c in random.sample(day1_customers, 20):
    c_new = c.copy()
    c_new["location"] = {"city": random.choice([city for city in CITIES if city != c["location"]["city"]]), "country": "Egypt"}
    c_new["updated_at"] = f"2026-08-21 09:30:00"
    day2_customers.append(c_new)

# Day 2: 50 completely new customers
new_customers = generate_customers(400, 50, "2026-08-21")
day2_customers.extend(new_customers)
all_active_ids = day1_cust_ids + [c["customer_id"] for c in new_customers]

# Day 2: 300 new orders
day2_orders = generate_orders(6000, 300, all_active_ids, "2026-08-21")

with open(os.path.join(DATA_DIR, 'customers_day2.json'), 'w', encoding='utf-8') as f:
    for c in day2_customers: f.write(json.dumps(c) + '\n')

with open(os.path.join(DATA_DIR, 'orders_day2.json'), 'w', encoding='utf-8') as f:
    for o in day2_orders: f.write(json.dumps(o) + '\n')

print("Success! Mock data generated.")
