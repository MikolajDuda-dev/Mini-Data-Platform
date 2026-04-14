from kafka import KafkaConsumer
import json

TOPIC_NAME = 'pg-server.public.orders'
BOOTSTRAP_SERVERS = ['localhost:9092']

print(f"Starting listening on topic: {TOPIC_NAME}...")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

try:
    for message in consumer:
        data = message.value

        if data and 'op' in data and data['op'] in ['c', 'r']:
            order = data['after']
            if order:
                print(f"[{data['op']}] Received: ID {order['order_id']} - {order['product_name']} (Price encoded: {order['price']})")
        
except KeyboardInterrupt:
    print("Consumer stopped.")
except Exception as e:
    print(f"An error occurred: {e}")