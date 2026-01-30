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

import ijson
import sys
from collections import defaultdict

def parse_with_ijson(filepath):
    """
    Parse large JSON file using ijson streaming parser.
    This never loads the entire file into memory.
    """
    stats = defaultdict(int)
    total_amount = 0
    count = 0
    
    print(f"Streaming parse of {filepath}...")
    
    with open(filepath, 'rb') as f:
        # ijson.items() yields objects at the specified path
        # 'transactions.item' means each item in the transactions array
        parser = ijson.items(f, 'transactions.item')
        
        for transaction in parser:
            count += 1
            total_amount += transaction['amount']
            stats[transaction['product']] += 1
            stats[transaction['metadata']['region']] += 1
            
            # Progress indicator
            if count % 10000 == 0:
                print(f"  Processed {count} records...")
    
    print(f"\n--- Results ---")
    print(f"Total transactions: {count}")
    print(f"Total amount: ${total_amount:,.2f}")
    print(f"Average transaction: ${total_amount/count:,.2f}")
    print(f"\nProduct distribution:")
    for product in ["iPhone 15", "Galaxy S24", "Pixel 8", "OnePlus 12"]:
        print(f"  {product}: {stats[product]}")

def compare_memory_usage():
    """Compare memory: standard json vs ijson"""
    import tracemalloc
    import json
    
    filepath = "large_transactions.json"
    
    # Method 1: Standard json.load (loads everything)
    tracemalloc.start()
    with open(filepath, 'r') as f:
        data = json.load(f)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Standard json.load - Peak memory: {peak / 1024 / 1024:.2f} MB")
    del data
    
    # Method 2: ijson streaming
    tracemalloc.start()
    with open(filepath, 'rb') as f:
        count = sum(1 for _ in ijson.items(f, 'transactions.item'))
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"ijson streaming   - Peak memory: {peak / 1024 / 1024:.2f} MB")

if __name__ == "__main__":
    print("=== Memory Comparison ===")
    compare_memory_usage()
    print("\n=== Full Parse with Stats ===")
    parse_with_ijson("large_transactions.json")
