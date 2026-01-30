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

from kafka import KafkaConsumer
import json
from collections import defaultdict

def consume_transactions(topic='transactions', max_messages=1000):
    """
    Consume messages from Kafka topic.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Start from beginning
        enable_auto_commit=True,
        group_id='transaction-processor',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000  # Stop after 10s of no messages
    )
    
    stats = defaultdict(float)
    count = 0
    
    print(f"Consuming from '{topic}'...")
    
    for message in consumer:
        transaction = message.value
        stats[transaction['product']] += transaction['amount']
        count += 1
        
        if count <= 3:
            print(f"  Sample: {transaction['transaction_id']} - "
                  f"{transaction['product']} - ${transaction['amount']}")
        
        if count >= max_messages:
            break
    
    consumer.close()
    
    print(f"\nProcessed {count} messages")
    print("\nRevenue by Product:")
    for product, revenue in sorted(stats.items(), key=lambda x: -x[1]):
        print(f"  {product}: ${revenue:,.2f}")

if __name__ == "__main__":
    consume_transactions()
