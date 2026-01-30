# kafka_consumer.py
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
