"""
CSE 512 — Assignment 1 (MongoDB Aggregation via PyMongo)
Author: Luna Sbahtu
Date: 2025-09-18

How to use
----------
1) Ensure MongoDB is running (e.g., via Docker):
   docker pull mongo:7
   docker run -d --name cse512-mongo -p 27017:27017 mongo:7

2) Create/activate your virtual environment, then install:
   pip install pymongo

3) Export 100 JSON rows from Mockaroo (array of objects) and set the path below.

4) Run:
   python assignment1_solution.py

Notes
-----
- By default this script connects to a local MongoDB at mongodb://localhost:27017
  You can override via the MONGODB_URI environment variable.
- The script creates/uses database "ecommerce" and collection "orders".
- It prints results for Parts 3–9 of the assignment.
"""

import os
import json
from typing import List, Dict, Any
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from pprint import pprint

# ==== Configuration ====
DATABASE_NAME = "ecommerce"
COLLECTION_NAME = "orders"

# Set this to your downloaded Mockaroo file (JSON array)
# Example: r"C:\Users\you\Downloads\mock_orders.json" or "./mock_orders.json"
MOCK_DATA_PATH = os.environ.get("MOCK_DATA_PATH", "./mock_orders.json")

# Mongo connection (use env var if provided, otherwise localhost)
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")


def get_collection() -> Collection:
    client = MongoClient(MONGODB_URI)
    db = client[DATABASE_NAME]
    return db[COLLECTION_NAME]


# ========== Part 3 ==========
def insert_mock_data(json_path: str = MOCK_DATA_PATH) -> None:
    """
    Inserts the generated Mockaroo data (JSON array) into MongoDB.
    Prints the inserted ObjectIds and collection count after insert.
    """
    col = get_collection()

    if not os.path.exists(json_path):
        print(f"[insert_mock_data] File not found: {json_path}")
        print("Please export your 100-row JSON array from Mockaroo and update MOCK_DATA_PATH.")
        return

    # Load JSON; expect an array of objects
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print("[insert_mock_data] Expected a JSON array of order objects.")
        return

    # Optional: clean collection before insert to avoid duplicates during re-runs
    # Comment out the next line if you prefer to keep previous data
    col.delete_many({})

    try:
        result = col.insert_many(data, ordered=False)
        print(f"[insert_mock_data] Inserted {len(result.inserted_ids)} documents.")
        print("Inserted _ids (first 10 shown):")
        for oid in result.inserted_ids[:10]:
            print(f"  {oid}")
    except BulkWriteError as bwe:
        print("[insert_mock_data] Bulk write error (likely duplicate keys). Summary:")
        print(bwe.details)


# ========== Part 4 ==========
def find_order_totals() -> None:
    """
    Finds the total number of orders and the number of orders per state,
    sorted by count in ascending order.
    """
    col = get_collection()

    total_orders = col.count_documents({})
    print(f"[find_order_totals] Total number of orders: {total_orders}")

    pipeline = [
        {"$group": {"_id": "$state", "count": {"$sum": 1}}},
        {"$sort": {"count": 1, "_id": 1}}  # ascending by count, tie-break by state name
    ]

    print("[find_order_totals] Orders per state (ascending by count):")
    for doc in col.aggregate(pipeline):
        state = doc["_id"]
        count = doc["count"]
        print(f"  {state:20s}  {count}")


# ========== Part 5 ==========
def find_product_frequencies() -> None:
    """
    Finds the products and their frequencies sorted by frequency in descending order.
    Assumes field 'product_id' exists (as per the sample).
    """
    col = get_collection()

    pipeline = [
        {"$group": {"_id": "$product_id", "frequency": {"$sum": 1}}},
        {"$sort": {"frequency": -1, "_id": 1}}
    ]

    print("[find_product_frequencies] product_id frequencies (descending):")
    for doc in col.aggregate(pipeline):
        pid = doc["_id"]
        freq = doc["frequency"]
        print(f"  product_id={pid:<6}  frequency={freq}")


# ========== Part 6 ==========
def ca_highvalue_orders(threshold: float = 1000.0) -> None:
    """
    Counts and prints orders in California with total_price > threshold.
    """
    col = get_collection()
    query = {"state": "California", "total_price": {"$gt": threshold}}

    count = col.count_documents(query)
    print(f"[ca_highvalue_orders] Count (California, total_price > {threshold}): {count}")

    # Print a few sample orders (limit 10 to keep console readable)
    print("[ca_highvalue_orders] Sample orders (up to 10):")
    for doc in col.find(query).limit(10):
        # Show key fields only for readability
        print({
            "order_id": doc.get("order_id"),
            "customer_id": doc.get("customer_id"),
            "total_price": doc.get("total_price"),
            "city": doc.get("city"),
            "order_date": doc.get("order_date")
        })


# ========== Part 7 ==========
def top_states_highvalue(threshold: float = 500.0, top_k: int = 10) -> None:
    """
    Finds the top K states with the most orders where total_price > threshold.
    Prints rank, state, and order count.
    """
    col = get_collection()

    pipeline = [
        {"$match": {"total_price": {"$gt": threshold}}},
        {"$group": {"_id": "$state", "order_count": {"$sum": 1}}},
        {"$sort": {"order_count": -1, "_id": 1}},
        {"$limit": top_k}
    ]

    print(f"[top_states_highvalue] Top {top_k} states (total_price > {threshold}):")
    rank = 1
    for doc in col.aggregate(pipeline):
        state = doc["_id"]
        cnt = doc["order_count"]
        print(f"  #{rank:<2} {state:20s} {cnt}")
        rank += 1


# ========== Part 8 ==========
def find_customer_premium(state: str = "Texas", threshold: float = 2000.0) -> None:
    """
    Counts and finds the customers who placed premium orders (total_price > threshold)
    in the given state. Prints distinct customer count and their info.
    """
    col = get_collection()

    pipeline = [
        {"$match": {"state": state, "total_price": {"$gt": threshold}}},
        {"$group": {"_id": "$customer_id", "orders": {"$push": "$$ROOT"}, "premium_count": {"$sum": 1}}},
        {"$sort": {"premium_count": -1, "_id": 1}}
    ]

    premium_customers: List[Dict[str, Any]] = list(col.aggregate(pipeline))
    print(f"[find_customer_premium] Distinct customers in {state} with total_price > {threshold}: {len(premium_customers)}")

    # Print a short summary per customer
    for entry in premium_customers[:10]:  # cap at 10 customers for console readability
        cid = entry["_id"]
        pcnt = entry["premium_count"]
        print(f"  customer_id={cid}  premium_orders={pcnt}")
        # Optionally show first order's brief info
        first_order = entry["orders"][0] if entry["orders"] else {}
        if first_order:
            print("    example_order:", {
                "order_id": first_order.get("order_id"),
                "total_price": first_order.get("total_price"),
                "city": first_order.get("city"),
                "order_date": first_order.get("order_date")
            })


# ========== Part 9 ==========
def find_orders_by_date(order_date: str, city_aliases: List[str] = None) -> None:
    """
    Counts and finds the orders placed in New York City on a specific date.
    order_date should match the string format used in your dataset, e.g., '10/21/2021'.
    Some datasets use 'New York' and others 'New York City'; we'll match both by default.
    """
    if city_aliases is None:
        city_aliases = ["New York City", "New York"]

    col = get_collection()
    query = {"order_date": order_date, "city": {"$in": city_aliases}}

    count = col.count_documents(query)
    print(f"[find_orders_by_date] NYC orders on {order_date}: {count}")
    for doc in col.find(query).limit(10):
        print({
            "order_id": doc.get("order_id"),
            "customer_id": doc.get("customer_id"),
            "total_price": doc.get("total_price"),
            "city": doc.get("city"),
            "order_date": doc.get("order_date")
        })


def main() -> None:
    # Run all parts in sequence. Adjust the order/date as needed.
    insert_mock_data()
    find_order_totals()
    find_product_frequencies()
    ca_highvalue_orders()
    top_states_highvalue()
    find_customer_premium()
    # Example date in the format used by your data
    specific_date = "1/9/2021"
    find_orders_by_date(specific_date)


if __name__ == "__main__":
    main()
