# Traffic Archiver: consumes Kafka events to simulate long-term storage.
import json
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Kafka consumer configuration shared by this archival service.
# BOOTSTRAP_SERVERS → Kafka broker address inside Docker
# TOPIC → Kafka topic where raw traffic events are stored
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("RAW_TOPIC", "raw_traffic_data")


def build_consumer():
    """Create a dedicated Kafka consumer used for long-term archival."""
    while True:
        try:
            # Create KafkaConsumer subscribed to the raw traffic topic
            # auto_offset_reset="earliest" → read events from the beginning
            # group_id="traffic_archiver" → unique consumer group for this service
            # value_deserializer → decode JSON payloads
            # key_deserializer → decode zone_id keys
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
            # Retry if Kafka is temporarily offline
            print("Kafka no disponible, reintentando archivador...")
            time.sleep(3)
        except Exception as exc:
            # Catch generic errors building the consumer
            print(f"Error creando consumidor: {exc}")
            time.sleep(3)


def main():
    """Continuously read Kafka events as a simulated archival process."""
    consumer = build_consumer()

    # Iterate over every incoming Kafka message
    for message in consumer:
        # Extract event payload and zone_id
        payload = message.value or {}
        zone_id = message.key or payload.get("zone_id")

        # Print to stdout to simulate saving into long-term storage
        # (e.g., data lake, database, cold storage)
        print(f"Archivando evento: zona={zone_id}, datos={payload}")


if __name__ == "__main__":
    # Start the archival consumer service.
    main()
