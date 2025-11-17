# Traffic Density Processor: materializes Kafka events and replicas state via RabbitMQ fanout.
import json
import os
import threading
import time

import pika
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Shared connection parameters for Kafka and RabbitMQ.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("RAW_TOPIC", "raw_traffic_data")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
BROADCAST_INTERVAL = max(1, int(os.getenv("BROADCAST_INTERVAL_SECONDS", "5")))

# In-memory KTable representing the current traffic state per zone.
zone_states = {}
state_lock = threading.Lock()


def classify_density(vehicle_count: int) -> str:
    """Map the raw vehicle count to the semantic traffic state."""
    if vehicle_count < 20:
        return "DESPEJADA"
    if vehicle_count < 40:
        return "MODERADA"
    return "CONGESTIONADA"


def build_consumer():
    """Attach a Kafka consumer that replays the entire topic when the service starts."""
    while True:
        try:
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
            print("Kafka no disponible, reintentando consumidor...")
            time.sleep(3)
        except Exception as exc:
            print(f"Error creando consumidor: {exc}")
            time.sleep(3)


def publish_snapshot(snapshot: dict):
    """Fan out the current materialized view using RabbitMQ so other services can replicate it."""
    try:
        parameters = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange="traffic_updates", exchange_type="fanout", durable=True)
        channel.basic_publish(exchange="traffic_updates", routing_key="", body=json.dumps(snapshot).encode("utf-8"))
        connection.close()
        print(f"Estado publicado a RabbitMQ ({len(snapshot)} zonas)")
    except pika.exceptions.AMQPConnectionError:
        print("RabbitMQ no disponible, reintentando pronto...")
    except Exception as exc:
        print(f"Error publicando a RabbitMQ: {exc}")


def broadcast_loop():
    """Publish a copy of the shared state at a fixed interval."""
    while True:
        time.sleep(BROADCAST_INTERVAL)
        with state_lock:
            snapshot = dict(zone_states)
        publish_snapshot(snapshot)


def main():
    """Continuously materialize Kafka events while broadcasting the shared state to RabbitMQ."""
    threading.Thread(target=broadcast_loop, daemon=True).start()
    consumer = build_consumer()
    for message in consumer:
        payload = message.value or {}
        zone_id = message.key or payload.get("zone_id") or "DESCONOCIDA"
        vehicle_count = int(payload.get("vehicle_count", 0))
        state = classify_density(vehicle_count)
        with state_lock:
            zone_states[zone_id] = state
        print(f"Zona {zone_id} -> {state} (vehiculos: {vehicle_count})")


if __name__ == "__main__":
    # Entry-point for the processing brain of the platform.
    main()
