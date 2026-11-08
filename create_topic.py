import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, InvalidTopicError
from configs import kafka_config, TOPIC_SENSORS, TOPIC_TEMPERATURE, TOPIC_HUMIDITY


sensor_id = random.randint(1000, 9999)

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    client_id="topic_creator",
)

desired = [
    NewTopic(name=TOPIC_SENSORS,
             num_partitions=2, replication_factor=1),
    NewTopic(name=TOPIC_TEMPERATURE,
             num_partitions=2, replication_factor=1),
    NewTopic(name=TOPIC_HUMIDITY,
             num_partitions=2, replication_factor=1),
]

existing = set(admin_client.list_topics())
to_create = [t for t in desired if t.name not in existing]
if to_create:
    try:
        admin_client.create_topics(new_topics=to_create, validate_only=False)
        print("Created topics:", [t.name for t in to_create])
    except TopicAlreadyExistsError:
        pass
    except InvalidTopicError as e:
        print("Invalid topic:", e)
    except Exception as e:
        print("Create topics failed:", e)
else:
    print("All desired topics already exist.")
admin_client.close()

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
    linger_ms=200,
)

print(
    f"Sensor {sensor_id} → topic '{TOPIC_SENSORS}' @ {kafka_config['bootstrap_servers']}")
try:
    for i in range(30):
        msg = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(random.uniform(25, 45), 2),
            "humidity": round(random.uniform(15, 85), 2),
        }
        producer.send(TOPIC_SENSORS, key=sensor_id, value=msg)
        print(f"[{i+1:02d}] sent: {msg}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    producer.flush()
    producer.close()
