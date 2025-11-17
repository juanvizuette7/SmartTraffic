# Alert Dispatcher: replicates state via fanout and answers queries via work queue + topic.
import json
import os
import threading
import time

import pika

# RabbitMQ connection details and queue/exchange names for the messaging patterns.
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUERY_QUEUE = "query_traffic_queue"
ANSWER_EXCHANGE = "query_answers"

# Local replica of the zone map used to serve user queries.
zone_states: dict[str, str] = {}
state_lock = threading.Lock()


def listen_for_updates():
    """Replicate the latest traffic snapshot from the RabbitMQ fanout exchange."""
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.exchange_declare(exchange="traffic_updates", exchange_type="fanout", durable=True)
            queue = channel.queue_declare(queue="", exclusive=True)
            queue_name = queue.method.queue
            channel.queue_bind(queue=queue_name, exchange="traffic_updates")
            print("Alert dispatcher replicando estado desde fanout")

            def callback(ch, method, properties, body):
                data = json.loads(body.decode("utf-8"))
                with state_lock:
                    zone_states.clear()
                    zone_states.update(data)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f"Estado replicado: {zone_states}")

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ (fanout) no disponible, reintentando...")
            time.sleep(3)
        except Exception as exc:
            print(f"Error en la replicacion de estado: {exc}")
            time.sleep(3)


def listen_for_queries():
    """Consume user queries from the work queue and answer them using the local replica."""
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=QUERY_QUEUE, durable=True)
            channel.exchange_declare(exchange=ANSWER_EXCHANGE, exchange_type="topic", durable=True)
            channel.basic_qos(prefetch_count=1)
            print("Alert dispatcher escuchando consultas de usuarios")

            def on_request(ch, method, properties, body):
                try:
                    request = json.loads(body.decode("utf-8"))
                    user_id = request.get("user_id", "anonimo")
                    trayecto = request.get("trayecto", [])
                    with state_lock:
                        snapshot = {zona: zone_states.get(zona, "DESCONOCIDO") for zona in trayecto}
                    response = {
                        "user_id": user_id,
                        "trayecto": trayecto,
                        "estado": snapshot,
                        "mensaje": "Respuesta generada por alert_dispatcher",
                    }
                    routing_key = f"answer.{user_id}"
                    channel.basic_publish(
                        exchange=ANSWER_EXCHANGE,
                        routing_key=routing_key,
                        body=json.dumps(response).encode("utf-8"),
                    )
                    print(f"Respuesta enviada ({routing_key}): {snapshot}")
                except Exception as exc:
                    print(f"Error procesando consulta: {exc}")
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=QUERY_QUEUE, on_message_callback=on_request)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ (work queue) no disponible, reintentando...")
            time.sleep(3)
        except Exception as exc:
            print(f"Error escuchando consultas: {exc}")
            time.sleep(3)


def main():
    """Start RabbitMQ consumers: one for replication and one for the work queue."""
    threading.Thread(target=listen_for_updates, daemon=True).start()
    listen_for_queries()


if __name__ == "__main__":
    try:
        # Run the dispatcher responsible for proactive alerts and query replies.
        main()
    except KeyboardInterrupt:
        print("Alert dispatcher detenido")
