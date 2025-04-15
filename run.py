import asyncio
from agent import agent_simulation, agent_fault_detection
import time
import socket
from pydecouple import config

host = config('RABBITMQ_HOST')
port = config('RABBITMQ_PORT')

print("Waiting for RabbitMQ to start...")
while True:
    try:
        with socket.create_connection((host, port), timeout=5):
            print("RabbitMQ is up - starting simulations...")
            break
    except (socket.timeout, ConnectionRefusedError, OSError):
        print("RabbitMQ is unavailable - retrying in 5 seconds...")
        time.sleep(5)

async def run_all():
    await asyncio.gather(
        agent_simulation.main(),
        agent_fault_detection.main()
    )

if __name__ == "__main__":
    asyncio.run(run_all())