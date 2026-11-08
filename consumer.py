from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, TOPIC_SENSORS, TOPIC_TEMPERATURE, TOPIC_HUMIDITY
import json

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
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

# Producer для алертів
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    acks="all"
)

# Назва топіків
topic_name = TOPIC_SENSORS
topic_temperature = TOPIC_TEMPERATURE
topic_humidity = TOPIC_HUMIDITY

# Підписка на тему
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:

        data = message.value

        if not all(
            field in data
            for field in ("sensor_id", "timestamp", "temperature", "humidity")
        ):
            print(f"Пропущено повідомлення іншого формату: {data}")
            continue

        # Перевірка температури
        if data["temperature"] > 40:

            msg_temperature_alerts = {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "temperature": data["temperature"],
                "humidity": data["humidity"],
                "message": "Temperature exceeds 40°C"
            }

            producer.send(
                topic_temperature,
                key=data["sensor_id"],
                value=msg_temperature_alerts
            )

        # Перевірка вологості
        if data["humidity"] > 80 or data["humidity"] < 20:

            alert_message = (
                "Humidity exceeds 80%"
                if data["humidity"] > 80
                else "Humidity is below 20%"
            )

            msg_humidity_alerts = {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "temperature": data["temperature"],
                "humidity": data["humidity"],
                "message": alert_message
            }

            producer.send(
                topic_humidity,
                key=data["sensor_id"],
                value=msg_humidity_alerts
            )


except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.flush()
    producer.close()
    consumer.close()
