# User Dashboard Backend: consumes RabbitMQ fanout and streams updates via WebSocket.
import asyncio
import contextlib
import json
import os
import aio_pika
import websockets

# RABBITMQ_URL → connection string for RabbitMQ
# WS_HOST / WS_PORT → WebSocket server host/port where dashboard connects
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT", "8000"))

# connected_clients → set of active websocket clients
# latest_state → latest city traffic snapshot cached in memory
connected_clients: set[websockets.WebSocketServerProtocol] = set()
latest_state: dict[str, str] = {}


async def send_safe(client: websockets.WebSocketServerProtocol, payload: str):
    """Send a message to a WebSocket client, remove it if connection is broken."""
    try:
        await client.send(payload)
    except Exception:
        connected_clients.discard(client)


async def consumer_loop():
    """Consume traffic updates from RabbitMQ fanout and refresh in-memory state."""
    while True:
        try:
            # Connect to RabbitMQ using a resilient async connection
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            channel = await connection.channel()

            # Declare the fanout exchange that broadcasts traffic updates
            exchange = await channel.declare_exchange(
                "traffic_updates", aio_pika.ExchangeType.FANOUT, durable=True
            )

            # Create an exclusive queue for this service and bind it to the fanout
            queue = await channel.declare_queue("", exclusive=True)
            await queue.bind(exchange)

            print("Escuchando actualizaciones de trafico")

            # Iterate over incoming messages indefinitely
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():

                        # Update latest_state snapshot with the incoming data
                        payload = json.loads(message.body.decode("utf-8"))
                        latest_state.clear()
                        latest_state.update(payload)

                        # Broadcast the new snapshot to all connected WebSocket clients
                        if connected_clients:
                            text = json.dumps(latest_state)
                            await asyncio.gather(
                                *(send_safe(client, text) for client in list(connected_clients)),
                                return_exceptions=True,
                            )
        except Exception as exc:
            # RabbitMQ unavailable → retry later
            print(f"Error consumiendo RabbitMQ: {exc}")
            await asyncio.sleep(3)


async def websocket_handler(websocket: websockets.WebSocketServerProtocol):
    """Register a WebSocket client and immediately send the latest state."""
    # Add client to active list
    connected_clients.add(websocket)
    print("Cliente WebSocket conectado")
    try:
        # If a snapshot exists, push it instantly to the client
        if latest_state:
            await websocket.send(json.dumps(latest_state))

        # Keep connection open until client disconnects
        await websocket.wait_closed()

    finally:
        # Remove client when connection closes
        connected_clients.discard(websocket)
        print("Cliente WebSocket desconectado")


async def main():
    """Run the RabbitMQ fanout consumer alongside the WebSocket server."""
    consumer_task = asyncio.create_task(consumer_loop())
    try:
        # Start WebSocket server for dashboard.html
        async with websockets.serve(
            websocket_handler, WS_HOST, WS_PORT, ping_interval=None
        ):
            print(f"WebSocket disponible en ws://{WS_HOST}:{WS_PORT}/ws")
            await asyncio.Future()  # Keep server running forever
    finally:
        # Stop the fanout consumer gracefully
        consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task


if __name__ == "__main__":
    try:
        # Entry point for the real-time dashboard backend
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Backend de dashboard detenido")
