import json
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("RAW_TOPIC", "raw_traffic_data")


def build_consumer():
    """Independent Kafka consumer used to simulate a cold-storage sink."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                group_id="traffic_archiver",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
            print("Consumidor Kafka para archivado listo")
            return consumer
        except NoBrokersAvailable:
            print("Kafka no disponible, reintentando archivador...")
            time.sleep(3)
        except Exception as exc:
            print(f"Error creando consumidor: {exc}")
            time.sleep(3)


def main():
    """Read every event from Kafka to emulate an archival pipeline."""
    consumer = build_consumer()
    for message in consumer:
        payload = message.value or {}
        zone_id = message.key or payload.get("zone_id")
        print(f"Archivando evento: zona={zone_id}, datos={payload}")


if __name__ == "__main__":
    # Launch the archival consumer.
    main()
