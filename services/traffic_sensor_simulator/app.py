# Traffic Sensor Simulator: publishes random traffic events to Kafka.
import json
import os
import random
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

# Kafka connection parameters shared across the simulator.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("RAW_TOPIC", "raw_traffic_data")
ZONE_IDS = [z.strip() for z in os.getenv("ZONE_IDS", "Zone A,Zone B,Zone C").split(",") if z.strip()]


def ensure_topic():
    """Create the Kafka topic on startup so every other service can rely on it."""
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            topic = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
            admin.create_topics([topic])
            admin.close()
            print(f"Topic '{TOPIC}' listo")
            break
        except TopicAlreadyExistsError:
            print(f"Topic '{TOPIC}' ya existe")
            break
        except NoBrokersAvailable:
            print("Kafka no disponible, reintentando crear topic...")
            time.sleep(3)
        except Exception as exc:
            print(f"Error al crear topic: {exc}")
            time.sleep(3)


def build_producer():
    """Keep trying to connect the Kafka producer until the broker is reachable."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Productor Kafka conectado")
            return producer
        except NoBrokersAvailable:
            print("Kafka no disponible, reintentando conectar productor...")
            time.sleep(3)


def main():
    """Generate random events forever to feed the stateful processor."""
    ensure_topic()
    producer = build_producer()
    while True:
        zone_id = random.choice(ZONE_IDS or ["Zone A"])
        vehicle_count = random.randint(0, 60)
        payload = {"zone_id": zone_id, "vehicle_count": vehicle_count, "timestamp": time.time()}
        try:
            producer.send(TOPIC, key=zone_id, value=payload)
            producer.flush()
            print(f"Evento enviado: {payload}")
        except Exception as exc:
            print(f"Error al enviar evento: {exc}")
            producer = build_producer()
        time.sleep(1)


if __name__ == "__main__":
    # This service acts as the traffic sensor simulator producer.
    main()
