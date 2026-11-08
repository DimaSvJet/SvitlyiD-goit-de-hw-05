from kafka import KafkaConsumer
import json
from configs import kafka_config, TOPIC_HUMIDITY

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda b: b.decode('utf-8') if b else None,
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='humidity_alerts_viewer'   # Ідентифікатор групи споживачів
)

# Назва топіку
topic_humidity = TOPIC_HUMIDITY


# Підписка на тему
consumer.subscribe([topic_humidity])

print(f"Subscribed to topic '{topic_humidity}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        if message.value["humidity"] > 80:
            print(
                f"Extrimly humidity: {message.value['humidity']}, time: {message.value['timestamp']}, sensor: {message.value['sensor_id']}")
        else:
            print(
                f"Extrimly dry: {message.value['humidity']}, time: {message.value['timestamp']}, sensor: {message.value['sensor_id']}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
