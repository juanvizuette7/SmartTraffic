import asyncio
import contextlib
import json
import os

import aio_pika
import websockets

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT", "8000"))

connected_clients: set[websockets.WebSocketServerProtocol] = set()
latest_state: dict[str, str] = {}


async def send_safe(client: websockets.WebSocketServerProtocol, payload: str):
    """Push a payload to a websocket client, dropping it if the socket is broken."""
    try:
        await client.send(payload)
    except Exception:
        connected_clients.discard(client)


async def consumer_loop():
    """Subscribe to the RabbitMQ fanout and keep the in-memory snapshot fresh."""
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            channel = await connection.channel()
            exchange = await channel.declare_exchange("traffic_updates", aio_pika.ExchangeType.FANOUT, durable=True)
            queue = await channel.declare_queue("", exclusive=True)
            await queue.bind(exchange)
            print("Escuchando actualizaciones de trafico")
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        payload = json.loads(message.body.decode("utf-8"))
                        latest_state.clear()
                        latest_state.update(payload)
                        if connected_clients:
                            text = json.dumps(latest_state)
                            await asyncio.gather(
                                *(send_safe(client, text) for client in list(connected_clients)),
                                return_exceptions=True,
                            )
        except Exception as exc:
            print(f"Error consumiendo RabbitMQ: {exc}")
            await asyncio.sleep(3)


async def websocket_handler(websocket: websockets.WebSocketServerProtocol):
    """Handle a new dashboard client and push the latest state immediately."""
    connected_clients.add(websocket)
    print("Cliente WebSocket conectado")
    try:
        if latest_state:
            await websocket.send(json.dumps(latest_state))
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)
        print("Cliente WebSocket desconectado")


async def main():
    """Run the RabbitMQ consumer and the websocket server side-by-side."""
    consumer_task = asyncio.create_task(consumer_loop())
    try:
        async with websockets.serve(websocket_handler, WS_HOST, WS_PORT, ping_interval=None):
            print(f"WebSocket disponible en ws://{WS_HOST}:{WS_PORT}/ws")
            await asyncio.Future()
    finally:
        consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task


if __name__ == "__main__":
    try:
        # Start the real-time dashboard backend.
       asyncio.run(main())
    except KeyboardInterrupt:
        print("Backend de dashboard detenido")
