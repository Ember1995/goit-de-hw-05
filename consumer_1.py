from kafka import KafkaConsumer
from configs import kafka_config
import json
from producer import producer

# Створення Kafka Consumer для читання данних про вологість та температуру
consumer_1 = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: json.loads(k.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_1'   # Ідентифікатор групи споживачів
)

# Назва топіку
building_sensors_topic = 'hanna_dunska_building_sensors'

# Підписка на тему
consumer_1.subscribe([building_sensors_topic])
print(f"Subscribed to topic '{building_sensors_topic}'")

# Перевірка температури
def check_temperature(message):
    temperature = message.value.get('temperature')
    if temperature is not None and temperature > 40:
        producer.send('hanna_dunska_temperature_alerts', key=message.key, value=message.value)
        print(f"High temperature alert: {temperature}°C from sensor {message.key} was detected")

# Перевірка вологості
def check_humidity(message):
    humidity = message.value.get('humidity')
    if humidity is not None and (humidity > 80 or humidity < 20):
        producer.send('hanna_dunska_humidity_alerts', key=message.key, value=message.value)
        print(f"Humidity alert: {humidity}% from sensor {message.key} was detected")

# Обробка повідомлень з топіку
try:
    for message in consumer_1:
        print(f"Received message: {message.value}, from sensor_id: {message.key}, partition: {message.partition}")
        check_temperature(message)
        check_humidity(message)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer_1.close()  # Закриття consumer

