import json
import csv
import signal
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException

config = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'order-processor-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False 
}

consumer = Consumer(config)
csv_file = 'processed_orders.csv'
csv_headers = ['timestamp', 'order_id', 'user_id', 'amount', 'status']

with open(csv_file, 'a', newline='') as f:
    writer = csv.writer(f)
    if f.tell() == 0:
        writer.writerow(csv_headers)

running = True

def shutdown(sig, frame):
    global running
    print("\nShutting down consumer...")
    running = False

signal.signal(signal.SIGINT, shutdown)

def consume_to_csv():
    try:
        consumer.subscribe(['orders'])
        print(f"📡 Subscribed to 'orders'. Writing to {csv_file}...")

        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Error: {msg.error()}")
                    break

            try:
                data = json.loads(msg.value().decode('utf-8'))
                with open(csv_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        data.get('timestamp'),
                        data.get('order_id'),
                        data.get('user_id'),
                        data.get('amount'),
                        data.get('status')
                    ])
                
                print(f"💾 Saved Order: {data['order_id']} from Partition: {msg.partition()}")

                consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"⚠️ Error processing message: {e}")

    finally:
        consumer.close()
        print("🛑 Consumer closed.")

if __name__ == "__main__":
    consume_to_csv()