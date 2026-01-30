# MIT License
#
# Copyright (c) 2026 Sarang68
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
