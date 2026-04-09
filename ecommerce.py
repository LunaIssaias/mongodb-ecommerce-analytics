from pymongo import MongoClient
import json
import random
from datetime import datetime, timedelta

# Database configuration
DATABASE_NAME = 'ecommerce'
COLLECTION_NAME = 'orders'

client = MongoClient('mongodb://localhost:27017/')
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# ─────────────────────────────────────────────
# Sample data generator
# ─────────────────────────────────────────────

PRODUCTS = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse', 'Headphones', 'Camera']
STATES = ['CA', 'TX', 'NY', 'FL', 'WA', 'IL', 'PA', 'OH', 'GA', 'NC']
CITIES = {
    'CA': 'Los Angeles', 'TX': 'Houston', 'NY': 'New York City',
    'FL': 'Miami', 'WA': 'Seattle', 'IL': 'Chicago',
    'PA': 'Philadelphia', 'OH': 'Columbus', 'GA': 'Atlanta', 'NC': 'Charlotte'
}


def insert_mock_data():
    """Inserts mock e-commerce order data into MongoDB."""
    collection.drop()
    orders = []
    base_date = datetime(2021, 1, 1)

    for i in range(500):
        state = random.choice(STATES)
        order_date = base_date + timedelta(days=random.randint(0, 365))
        orders.append({
            'order_id': f'ORD-{i+1:04d}',
            'customer': f'Customer_{random.randint(1, 200)}',
            'product': random.choice(PRODUCTS),
            'amount': round(random.uniform(50, 3000), 2),
            'state': state,
            'city': CITIES[state],
            'order_date': order_date.strftime('%m/%d/%Y')
        })

    collection.insert_many(orders)
    print(f"Inserted {len(orders)} orders into '{DATABASE_NAME}.{COLLECTION_NAME}'")


def find_order_totals():
    """Finds the total number of orders and number of orders per state, sorted ascending."""
    total = collection.count_documents({})
    print(f"\nTotal Orders: {total}")

    pipeline = [
        {'$group': {'_id': '$state', 'count': {'$sum': 1}}},
        {'$sort': {'count': 1}}
    ]
    results = list(collection.aggregate(pipeline))
    print("\nOrders per State (ascending):")
    for r in results:
        print(f"  {r['_id']}: {r['count']} orders")


def find_product_frequencies():
    """Finds products and their frequencies sorted by frequency descending."""
    pipeline = [
        {'$group': {'_id': '$product', 'frequency': {'$sum': 1}}},
        {'$sort': {'frequency': -1}}
    ]
    results = list(collection.aggregate(pipeline))
    print("\nProduct Frequencies:")
    for r in results:
        print(f"  {r['_id']}: {r['frequency']}")


def ca_highvalue_orders():
    """Counts and finds orders in California where amount exceeds $1,000."""
    query = {'state': 'CA', 'amount': {'$gt': 1000}}
    results = list(collection.find(query, {'_id': 0, 'order_id': 1, 'customer': 1, 'amount': 1}))
    print(f"\nHigh-Value Orders in CA (> $1,000): {len(results)}")
    for r in results[:5]:
        print(f"  {r['order_id']} | {r['customer']} | ${r['amount']:.2f}")


def top_states_highvalue():
    """Finds the top 10 states with the most orders where amount exceeds $500."""
    pipeline = [
        {'$match': {'amount': {'$gt': 500}}},
        {'$group': {'_id': '$state', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]
    results = list(collection.aggregate(pipeline))
    print("\nTop 10 States with Orders > $500:")
    for r in results:
        print(f"  {r['_id']}: {r['count']} orders")


def find_customer_premium():
    """Counts and finds customers who placed premium orders (> $2,000) in Texas."""
    query = {'state': 'TX', 'amount': {'$gt': 2000}}
    results = list(collection.find(query, {'_id': 0, 'customer': 1, 'amount': 1, 'product': 1}))
    print(f"\nPremium Customers in TX (> $2,000): {len(results)}")
    for r in results[:5]:
        print(f"  {r['customer']} | {r['product']} | ${r['amount']:.2f}")


def find_orders_by_date(order_date):
    """Counts and finds orders placed in New York City on a specific date."""
    query = {'city': 'New York City', 'order_date': order_date}
    results = list(collection.find(query, {'_id': 0}))
    print(f"\nOrders in NYC on {order_date}: {len(results)}")
    for r in results:
        print(f"  {r['order_id']} | {r['customer']} | ${r['amount']:.2f}")


if __name__ == '__main__':
    insert_mock_data()
    find_order_totals()
    find_product_frequencies()
    ca_highvalue_orders()
    top_states_highvalue()
    find_customer_premium()

    specific_date = '1/9/2021'
    find_orders_by_date(specific_date)
