import json
from decimal import Decimal
from kafka import KafkaProducer
import ijson

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v, cls=DecimalEncoder).encode('utf-8')
)

def stream_json_to_kafka(file_path, topic='transactions'):
    print(f"Streaming {file_path} to Kafka topic '{topic}'...")
    
    with open(file_path, 'rb') as file:
        parser = ijson.items(file, 'item')
        count = 0
        
        for transaction in parser:
            key = transaction.get('transaction_id', 'unknown')
            
            # Send to Kafka
            future = producer.send(topic, key=key, value=transaction)
            
            count += 1
            if count % 1000 == 0:
                print(f"Sent {count} transactions...")
                producer.flush()  # Ensure delivery
        
        producer.flush()
        print(f"✓ Completed! Sent {count} total transactions")

if __name__ == "__main__":
    stream_json_to_kafka("large_transactions.json")
    producer.close()
