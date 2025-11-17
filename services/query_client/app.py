# Query Client: publishes route questions and listens for topic responses.
import json
import os
import random
import threading
import time

import pika

# RabbitMQ endpoints and client-specific parameters.
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUERY_QUEUE = "query_traffic_queue"
ANSWER_EXCHANGE = "query_answers"
CLIENT_ID = os.getenv("QUERY_CLIENT_ID", "client_001")
QUERY_INTERVAL = max(5, int(os.getenv("QUERY_INTERVAL_SECONDS", "15")))
ROUTE_ZONES = [z.strip() for z in os.getenv("QUERY_ROUTE", "Zone A|Zone B").split("|") if z.strip()]


def receive_responses():
    """Consume responses routed to this client via the topic exchange."""
    routing_key = f"answer.{CLIENT_ID}"
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.exchange_declare(exchange=ANSWER_EXCHANGE, exchange_type="topic", durable=True)
            queue = channel.queue_declare(queue="", exclusive=True)
            queue_name = queue.method.queue
            channel.queue_bind(queue=queue_name, exchange=ANSWER_EXCHANGE, routing_key=routing_key)
            print(f"Esperando respuestas en {routing_key}")

            def on_response(ch, method, properties, body):
                try:
                    data = json.loads(body.decode("utf-8"))
                    estado = data.get("estado", {})
                    resumen = ", ".join(f"{zona}: {estado.get(zona, 'DESCONOCIDO')}" for zona in data.get("trayecto", []))
                    print(f"RESPUESTA ASINCRONA ({routing_key}): {resumen}")
                except Exception as exc:
                    print(f"Error al procesar respuesta: {exc}")
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=queue_name, on_message_callback=on_response)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ no disponible para respuestas, reintentando...")
            time.sleep(3)
        except Exception as exc:
            print(f"Error escuchando respuestas: {exc}")
            time.sleep(3)


def send_queries():
    """Publish route queries at a fixed cadence using the work queue pattern."""
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=QUERY_QUEUE, durable=True)
            print("Query client enviando consultas periodicas")
            while True:
                base_route = ROUTE_ZONES or ["Zone A"]
                sample_size = random.randint(1, len(base_route))
                trayecto = random.sample(base_route, k=sample_size)
                mensaje = {
                    "user_id": CLIENT_ID,
                    "trayecto": trayecto,
                    "timestamp": time.time(),
                }
                channel.basic_publish(exchange="", routing_key=QUERY_QUEUE, body=json.dumps(mensaje).encode("utf-8"))
                print(f"Consulta enviada: {mensaje}")
                time.sleep(QUERY_INTERVAL)
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ no disponible para consultas, reintentando...")
            time.sleep(3)
        except Exception as exc:
            print(f"Error enviando consulta: {exc}")
            time.sleep(3)


def main():
    """Start the background listener and continuously issue new queries."""
    threading.Thread(target=receive_responses, daemon=True).start()
    send_queries()


if __name__ == "__main__":
    try:
        # Launch the simulated asynchronous client.
        main()
    except KeyboardInterrupt:
        print("Query client detenido")
