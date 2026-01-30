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
