import json
from kafka import KafkaConsumer
from configs import kafka_config, TOPIC_TEMPERATURE, TOPIC_HUMIDITY

consumer = KafkaConsumer(
    TOPIC_TEMPERATURE,
    TOPIC_HUMIDITY,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="alerts_viewer_svitlyi"
)

try:
    for record in consumer:
        print(f"Topic: {record.topic}")
        print(f"Sensor: {record.value['sensor_id']}")
        print(f"Time: {record.value['timestamp']}")
        print(f"Message: {record.value['message']}")
        print("-" * 40)

except KeyboardInterrupt:
    print("Stopped.")

finally:
    consumer.close()
