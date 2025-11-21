# Traffic Density Processor: materializes Kafka events and replicas state via RabbitMQ fanout.
import json
import os
import threading
import time

import pika
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Shared connection parameters for Kafka and RabbitMQ.
# BOOTSTRAP_SERVERS → Address of the Kafka broker inside Docker
# TOPIC → Kafka topic holding raw traffic events
# RABBITMQ_URL → Connection URL for RabbitMQ
# BROADCAST_INTERVAL → How often the processor sends the full state snapshot
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("RAW_TOPIC", "raw_traffic_data")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
BROADCAST_INTERVAL = max(1, int(os.getenv("BROADCAST_INTERVAL_SECONDS", "5")))

# In-memory KTable representing the current traffic state per zone.
# zone_states → Materialized view with latest traffic state for each zone
# state_lock → Ensures safe updates from multiple threads
zone_states = {}
state_lock = threading.Lock()


def classify_density(vehicle_count: int) -> str:
    """Classify raw vehicle_count into a semantic state (DESPEJADA/MODERADA/CONGESTIONADA)."""
    if vehicle_count < 20:
        return "DESPEJADA"
    if vehicle_count < 40:
        return "MODERADA"
    return "CONGESTIONADA"


def build_consumer():
    """Create a Kafka consumer that replays the entire topic on startup."""
    while True:
        try:
            # Subscribes to the RAW_TOPIC and reads messages from the earliest offset.
            # group_id identifies this processor as a unique consumer group.
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                group_id="traffic_density_processor",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                enable_auto_commit=True,
            )
            print("Consumidor Kafka listo")
            return consumer
        except NoBrokersAvailable:
            # Kafka unavailable → retry until broker comes online
            print("Kafka no disponible, reintentando consumidor...")
            time.sleep(3)
        except Exception as exc:
            # Any other error while creating the consumer
            print(f"Error creando consumidor: {exc}")
            time.sleep(3)


def publish_snapshot(snapshot: dict):
    """Publish the current materialized state via RabbitMQ fanout so other services can copy it."""
    try:
        # Connect to RabbitMQ and publish a full snapshot of zone_states
        parameters = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Fanout exchange → broadcast to ALL bound queues (dashboard + dispatcher)
        channel.exchange_declare(exchange="traffic_updates", exchange_type="fanout", durable=True)
        channel.basic_publish(
            exchange="traffic_updates",
            routing_key="",              # Fanout ignores the routing key
            body=json.dumps(snapshot).encode("utf-8")  # Serialized snapshot
        )
        connection.close()
        print(f"Estado publicado a RabbitMQ ({len(snapshot)} zonas)")
    except pika.exceptions.AMQPConnectionError:
        # RabbitMQ offline, retry later
        print("RabbitMQ no disponible, reintentando pronto...")
    except Exception as exc:
        # Catch any publish error
        print(f"Error publicando a RabbitMQ: {exc}")


def broadcast_loop():
    """Background loop that sends a snapshot every BROADCAST_INTERVAL seconds."""
    while True:
        time.sleep(BROADCAST_INTERVAL)

        # Copy shared state while holding the lock to avoid race conditions
        with state_lock:
            snapshot = dict(zone_states)

        publish_snapshot(snapshot)


def main():
    """Consume Kafka events and update state while broadcasting snapshots via fanout."""
    # Background thread → periodically sends the full state to RabbitMQ
    threading.Thread(target=broadcast_loop, daemon=True).start()

    consumer = build_consumer()

    # Consume Kafka events continuously
    for message in consumer:
        # Parse each record from Kafka (zone_id + vehicle_count)
        payload = message.value or {}
        zone_id = message.key or payload.get("zone_id") or "DESCONOCIDA"
        vehicle_count = int(payload.get("vehicle_count", 0))

        # Translate raw number → traffic state
        state = classify_density(vehicle_count)

        # Update the shared KTable safely
        with state_lock:
            zone_states[zone_id] = state

        print(f"Zona {zone_id} -> {state} (vehiculos: {vehicle_count})")


if __name__ == "__main__":
    # Entry-point for the processing brain of the platform.
    main()
