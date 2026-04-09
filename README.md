# MongoDB E-Commerce Analytics

A data analytics system built on MongoDB that queries and analyzes e-commerce order data across U.S. states — covering order volumes, product demand, high-value customers, and geographic trends.

## Features

- Inserts and manages order data in MongoDB
- Aggregates order counts by state
- Ranks products by purchase frequency
- Filters high-value orders by state and amount threshold
- Identifies premium customers in specific regions
- Queries orders by city and date

## Queries

| Function | Description |
|----------|-------------|
| `find_order_totals()` | Total orders and breakdown per state |
| `find_product_frequencies()` | Products ranked by order frequency |
| `ca_highvalue_orders()` | California orders exceeding $1,000 |
| `top_states_highvalue()` | Top 10 states with orders over $500 |
| `find_customer_premium()` | Premium Texas customers (orders > $2,000) |
| `find_orders_by_date()` | NYC orders filtered by specific date |

## Usage

```bash
pip install -r requirements.txt
python ecommerce.py
```

> Requires a running MongoDB instance at `localhost:27017`.

## Tech Stack

- Python
- MongoDB
- PyMongo
