import os
import json
import random

# ============================================================
# SnowMart Data Generator
# Generates realistic e-commerce JSON data for pipeline testing.
# Run this script before executing the pipeline for the first time,
# and again when you want to simulate an incremental (Day 2) load.
# ============================================================

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INITIAL_DIR  = os.path.join(REPO_ROOT, 'data', 'initial_load')
INCREMENTAL_DIR = os.path.join(REPO_ROOT, 'data', 'incremental_load')

os.makedirs(INITIAL_DIR, exist_ok=True)
os.makedirs(INCREMENTAL_DIR, exist_ok=True)

CITIES      = ['Cairo', 'Alexandria', 'Giza', 'Mansoura', 'Luxor', 'Aswan', 'Tanta', 'Port Said']
ITEMS       = [('Laptop', 1200.00), ('Mouse', 25.00), ('Keyboard', 45.00),
               ('Monitor', 300.00), ('Headphones', 150.00), ('Tablet', 600.00), ('Cover', 30.00)]
FIRST_NAMES = ['Ahmed', 'Mohamed', 'Mahmoud', 'Sarah', 'Mona', 'Nour', 'Omar', 'Youssef', 'Salma', 'Hala']
LAST_NAMES  = ['Ali', 'Hassan', 'Ibrahim', 'Tarek', 'Kamal', 'Mostafa', 'Fathy', 'Saad']


def make_customers(start_id, count, date_str):
    customers = []
    for i in range(count):
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        customers.append({
            'customer_id': start_id + i,
            'name': f'{fname} {lname}',
            'email': f'{fname.lower()}.{lname.lower()}{start_id + i}@email.com',
            'location': {'city': random.choice(CITIES), 'country': 'Egypt'},
            'updated_at': f'{date_str} {random.randint(8, 12):02d}:{random.randint(0, 59):02d}:00'
        })
    return customers


def make_orders(start_id, count, customer_ids, date_str):
    orders = []
    for i in range(count):
        items = []
        for _ in range(random.randint(1, 3)):
            item_name, price = random.choice(ITEMS)
            items.append({'item_name': item_name, 'price': price, 'qty': random.randint(1, 2)})
        orders.append({
            'order_id': start_id + i,
            'customer_id': random.choice(customer_ids),
            'items': items,
            'order_date': f'{date_str} {random.randint(10, 22):02d}:{random.randint(0, 59):02d}:00'
        })
    return orders


def write_jsonl(records, path):
    with open(path, 'w', encoding='utf-8') as f:
        for r in records:
            f.write(json.dumps(r) + '\n')


# ---- Initial Load (Day 1) ----
print('Generating initial load data...')
day1_customers = make_customers(100, 200, '2026-08-20')
day1_ids = [c['customer_id'] for c in day1_customers]
day1_orders = make_orders(5000, 500, day1_ids, '2026-08-20')

write_jsonl(day1_customers, os.path.join(INITIAL_DIR, 'customers.json'))
write_jsonl(day1_orders,    os.path.join(INITIAL_DIR, 'orders.json'))
print(f'  Written {len(day1_customers)} customers and {len(day1_orders)} orders to data/initial_load/')

# ---- Incremental Load (Day 2) ----
# Simulates real CDC scenarios:
#   - 20 existing customers change their city (triggers SCD Type 2 update)
#   - 50 brand new customers (triggers SCD Type 2 insert)
#   - 300 new orders spread across all known customers
print('Generating incremental load data (CDC simulation)...')
day2_customers = []

# Existing customers with changed attribute (city or email)
for c in random.sample(day1_customers, 20):
    updated = c.copy()
    updated['location'] = {
        'city': random.choice([city for city in CITIES if city != c['location']['city']]),
        'country': 'Egypt'
    }
    updated['updated_at'] = '2026-08-21 09:30:00'
    day2_customers.append(updated)

# New customers
new_customers = make_customers(400, 50, '2026-08-21')
day2_customers.extend(new_customers)

all_ids = day1_ids + [c['customer_id'] for c in new_customers]
day2_orders = make_orders(6000, 300, all_ids, '2026-08-21')

write_jsonl(day2_customers, os.path.join(INCREMENTAL_DIR, 'customers.json'))
write_jsonl(day2_orders,    os.path.join(INCREMENTAL_DIR, 'orders.json'))
print(f'  Written {len(day2_customers)} customers and {len(day2_orders)} orders to data/incremental_load/')
print('\nDone. Upload the files to your Snowflake internal stage before running the pipeline.')
