import asyncio
from agent import agent_simulation, agent_fault_detection
import time
import socket
from decouple import config

host = config('RABBITMQ_HOST')
port = config('RABBITMQ_PORT')
db_host = config('TIMESCALEDB_HOST')
db_port = config('TIMESCALEDB_PORT')

print("Waiting for RabbitMQ and TimescaleDB to start...")
print("RabbitMQ host: ", host, " port: ", port)
print("TimescaleDB host: ", db_host, " port: ", db_port)
while True:
    try:
        with socket.create_connection((db_host, db_port), timeout=5):
            print("TimescaleDB is up ")
    except ConnectionRefusedError as err:
        print("Shimata, we got : ", err , " error.... eeeehhh.. Naze??")
        print("TimescaleDB is unavailable - retrying in 5 seconds...")
        time.sleep(5)
    try:
        with socket.create_connection((host, port), timeout=5):
            print("RabbitMQ is up ")
            break
    except ConnectionRefusedError as err:
        print("Shimata, we got : ", err , " error.... eeeehhh.. Naze??")
        print("RabbitMQ is unavailable - retrying in 5 seconds...")
        time.sleep(5)

async def run_all():
    await asyncio.gather(
        agent_simulation.main(),
        agent_fault_detection.main()
    )

if __name__ == "__main__":
    asyncio.run(run_all())