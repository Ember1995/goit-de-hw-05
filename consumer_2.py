from kafka import KafkaConsumer
from configs import kafka_config
import json

# Створення Kafka Consumer для читання алертів про температуру
consumer_2 = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: json.loads(k.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_2'   # Ідентифікатор групи споживачів
)

# Назва топіку
temperature_alerts_topic = 'hanna_dunska_temperature_alerts'

# Підписка на тему
consumer_2.subscribe([temperature_alerts_topic])
print(f"Subscribed to topic '{temperature_alerts_topic}'")

# Обробка повідомлень з топіку
try:
    for message in consumer_2:
        print(f"High temperature alert! Received measures {message.value} from sensor_id {message.key}.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer_2.close()  # Закриття consumer
