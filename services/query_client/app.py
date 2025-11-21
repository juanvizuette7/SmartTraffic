# Query Client: publishes route questions and listens for topic responses.
import json
import os
import random
import threading
import time
import pika

# RabbitMQ connection settings and client parameters.
# RABBITMQ_URL → Broker address
# QUERY_QUEUE → Queue where user questions are sent (work queue)
# ANSWER_EXCHANGE → Topic exchange used for directed replies
# CLIENT_ID → Unique ID to route responses back to this client
# QUERY_INTERVAL → How often queries are sent
# ROUTE_ZONES → Base list of zones used to build random itineraries
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUERY_QUEUE = "query_traffic_queue"
ANSWER_EXCHANGE = "query_answers"
CLIENT_ID = os.getenv("QUERY_CLIENT_ID", "client_001")
QUERY_INTERVAL = max(5, int(os.getenv("QUERY_INTERVAL_SECONDS", "15")))
ROUTE_ZONES = [z.strip() for z in os.getenv("QUERY_ROUTE", "Zone A|Zone B").split("|") if z.strip()]


def receive_responses():
    """Consume responses from the topic exchange routed only to this client."""
    routing_key = f"answer.{CLIENT_ID}"
    while True:
        try:
            # Connect to RabbitMQ
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            # Ensure the topic exchange exists
            channel.exchange_declare(exchange=ANSWER_EXCHANGE, exchange_type="topic", durable=True)

            # Create a private exclusive queue for this client
            queue = channel.queue_declare(queue="", exclusive=True)
            queue_name = queue.method.queue

            # Bind queue to only messages addressed to this client's routing key
            channel.queue_bind(queue=queue_name, exchange=ANSWER_EXCHANGE, routing_key=routing_key)
            print(f"Esperando respuestas en {routing_key}")

            # Callback to process each reply
            def on_response(ch, method, properties, body):
                try:
                    data = json.loads(body.decode("utf-8"))
                    estado = data.get("estado", {})
                    resumen = ", ".join(
                        f"{zona}: {estado.get(zona, 'DESCONOCIDO')}"
                        for zona in data.get("trayecto", [])
                    )
                    print(f"RESPUESTA ASINCRONA ({routing_key}): {resumen}")
                except Exception as exc:
                    print(f"Error al procesar respuesta: {exc}")
                finally:
                    # Acknowledge the message as processed
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            # Start consuming directed responses
            channel.basic_consume(queue=queue_name, on_message_callback=on_response)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            # If RabbitMQ is down, retry later
            print("RabbitMQ no disponible para respuestas, reintentando...")
            time.sleep(3)
        except Exception as exc:
            print(f"Error escuchando respuestas: {exc}")
            time.sleep(3)


def send_queries():
    """Publish route queries at fixed intervals using the work queue pattern."""
    while True:
        try:
            # Connect to RabbitMQ
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            # Ensure the work queue exists for user questions
            channel.queue_declare(queue=QUERY_QUEUE, durable=True)
            print("Query client enviando consultas periodicas")

            while True:
                # Zones to pick from for this query
                base_route = ROUTE_ZONES or ["Zone A"]

                # Random number of zones to simulate different questions
                sample_size = random.randint(1, len(base_route))

                # Build the simulated user question
                trayecto = random.sample(base_route, k=sample_size)

                mensaje = {
                    "user_id": CLIENT_ID,
                    "trayecto": trayecto,
                    "timestamp": time.time(),
                }

                # Publish question to the work queue
                channel.basic_publish(
                    exchange="",
                    routing_key=QUERY_QUEUE,
                    body=json.dumps(mensaje).encode("utf-8")
                )

                print(f"Consulta enviada: {mensaje}")
                time.sleep(QUERY_INTERVAL)

        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ no disponible para consultas, reintentando...")
            time.sleep(3)
        except Exception as exc:
            print(f"Error enviando consulta: {exc}")
            time.sleep(3)


def main():
    """Start the response listener, then continuously send queries."""
    threading.Thread(target=receive_responses, daemon=True).start()
    send_queries()


if __name__ == "__main__":
    try:
        # Launch the simulated asynchronous client.
        main()
    except KeyboardInterrupt:
        print("Query client detenido")
