from kafka import KafkaConsumer
from configs import kafka_config
import json

# Створення Kafka Consumer для читання алертів про вологість
consumer_3 = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: json.loads(k.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

# Назва топіку
humidity_alerts_topic = 'hanna_dunska_humidity_alerts'

# Підписка на тему
consumer_3.subscribe([humidity_alerts_topic])
print(f"Subscribed to topic '{humidity_alerts_topic}'")

# Обробка повідомлень з топіку
try:
    for message in consumer_3:
        print(f"Humidity alert! Received measures {message.value} from sensor_id {message.key}.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer_3.close()  # Закриття consumer