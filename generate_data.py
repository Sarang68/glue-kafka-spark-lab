# generate_data.py
import json
from faker import Faker
import random

fake = Faker()

def generate_transactions(num_records=10000):
    """Generate sample transaction data"""
    transactions = []
    for i in range(num_records):
        transactions.append({
            "transaction_id": f"TXN-{i:08d}",
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "product": random.choice(["iPhone 15", "Galaxy S24", "Pixel 8", "OnePlus 12"]),
            "amount": round(random.uniform(100, 1500), 2),
            "timestamp": fake.date_time_this_year().isoformat(),
            "store_id": f"STORE-{random.randint(100, 500)}",
            "payment_method": random.choice(["credit", "debit", "cash", "digital_wallet"]),
            "metadata": {
                "region": random.choice(["Northeast", "Southeast", "Midwest", "West"]),
                "channel": random.choice(["online", "in-store", "mobile_app"]),
                "promotion_applied": random.choice([True, False])
            }
        })
    return {"transactions": transactions}

if __name__ == "__main__":
    data = generate_transactions(50000)  # 50K records
    with open("large_transactions.json", "w") as f:
        json.dump(data, f)
    print("Generated large_transactions.json with 50,000 records")
