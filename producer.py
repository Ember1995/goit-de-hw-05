from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: json.dumps(k).encode('utf-8')
)


if __name__ == "__main__":
    
    # Назва топіку та ID пристрою
    topic_name = 'hanna_dunska_building_sensors'
    sensor_id = str(uuid.uuid4())

    for message in range(10):
        # Відправлення повідомлення в топік
        try:
            data = {
                "timestamp": time.time(),  # Часова мітка
                "temperature": random.randint(25, 45),  # Випадкове значення
                "humidity": random.randint(15, 85)  # Випадкове значення
            }
            
            producer.send(topic_name, key=sensor_id, value=data)
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
            print(f"Message {message} with measures {data} sent to topic '{topic_name}' successfully.")
            time.sleep(1)

        except Exception as e:
            print(f"An error occurred: {e}")

    producer.close()  # Закриття producer

