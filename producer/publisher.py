import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'm4-mac-producer',
    
    'acks': 'all', 
    'enable.idempotence': True,
    'retries': 5, 
    
    'linger.ms': 20,
    'compression.type': 'lz4',
    'batch.size': 32768 
}

p = Producer(config)

def delivery_report(err, msg):
    """Callback for delivery results"""
    if err is not None:
        print(f"Failed: {err}")
    else:
        print(f"✅ {msg.topic()} [{msg.partition()}] @ {msg.offset()}: {msg.key().decode('utf-8')}")

def simulate_stream(limit=100):
    topics = ['orders']
    
    for i in range(limit):
        order_id = f"ord_{random.randint(1000, 9999)}"
        payload = {
            'timestamp': datetime.utcnow().isoformat(),
            'order_id': order_id,
            'user_id': f"user_{random.randint(1, 500)}",
            'amount': round(random.uniform(10.0, 500.0), 2),
            'status': 'SUCCESS'
        }

        key = payload['user_id']
        
        try:
            p.produce(
                topic=random.choice(topics),
                key=key.encode('utf-8'),
                value=json.dumps(payload).encode('utf-8'),
                callback=delivery_report
            )
        except BufferError:
            print("Queue full, waiting...")
            p.poll(1)

        p.poll(0) 
        
    print("Flushing final messages...")
    p.flush()

if __name__ == "__main__":
    while True:
        simulate_stream(10)
        time.sleep(2)
