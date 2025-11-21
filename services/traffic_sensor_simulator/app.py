# Traffic Sensor Simulator: publishes random traffic events to Kafka.
import json
import os
import random
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

# BOOTSTRAP_SERVERS → Kafka address inside Docker
# TOPIC → Kafka topic where raw traffic events are published
# ZONE_IDS → List of simulated zones to randomize payloads
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("RAW_TOPIC", "raw_traffic_data")
ZONE_IDS = [z.strip() for z in os.getenv("ZONE_IDS", "Zone A,Zone B,Zone C").split(",") if z.strip()]


def ensure_topic():
    """Create the Kafka topic at startup so all services can rely on its existence."""
    while True:
        try:
            # Create a single-partition topic (system of record for raw events)
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            topic = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
            admin.create_topics([topic])
            admin.close()
            print(f"Topic '{TOPIC}' listo")
            break
        except TopicAlreadyExistsError:
            # Topic already exists → safe to continue
            print(f"Topic '{TOPIC}' ya existe")
            break
        except NoBrokersAvailable:
            # Kafka unavailable → retry until broker is ready
            print("Kafka no disponible, reintentando crear topic...")
            time.sleep(3)
        except Exception as exc:
            # Any other error creating the topic
            print(f"Error al crear topic: {exc}")
            time.sleep(3)


def build_producer():
    """Keep trying to create a Kafka producer until the broker becomes reachable."""
    while True:
        try:
            # Producer encodes key/value to JSON strings
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Productor Kafka conectado")
            return producer
        except NoBrokersAvailable:
            # Broker offline → retry
            print("Kafka no disponible, reintentando conectar productor...")
            time.sleep(3)


def main():
    """Continuously generate random sensor events and publish them to Kafka."""
    ensure_topic()            # Make sure the topic exists before producing
    producer = build_producer()

    while True:
        # Randomly pick a zone and generate a vehicle count
        zone_id = random.choice(ZONE_IDS or ["Zone A"])
        vehicle_count = random.randint(0, 60)

        # Sensor event payload
        payload = {"zone_id": zone_id, "vehicle_count": vehicle_count, "timestamp": time.time()}

        try:
            # Send event to Kafka (key = zone_id, value = JSON payload)
            producer.send(TOPIC, key=zone_id, value=payload)
            producer.flush()  # Ensure delivery
            print(f"Evento enviado: {payload}")
        except Exception as exc:
            # If producer fails, rebuild it
            print(f"Error al enviar evento: {exc}")
            producer = build_producer()

        # Produce one event per second
        time.sleep(1)


if __name__ == "__main__":
    # Entry point for the simulated traffic sensor producer
    main()
