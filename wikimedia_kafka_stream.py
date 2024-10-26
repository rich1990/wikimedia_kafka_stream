import asyncio
import aiohttp
from aiokafka import AIOKafkaProducer
import json
import time

# Configuration
KAFKA_BROKER = 'localhost:9092'  # Your Kafka broker address
KAFKA_TOPIC = 'my-topic'  # Desired topic name
MAX_RETRIES = 5  # Number of retries for connection issues
RETRY_DELAY = 2  # Delay in seconds between retries

async def send_to_kafka(producer, message):
    await producer.send_and_wait(KAFKA_TOPIC, message)

async def fetch_wikimedia_changes(producer):
    async with aiohttp.ClientSession() as session:
        retries = 0
        while retries < MAX_RETRIES:
            try:
                async with session.get("https://stream.wikimedia.org/v2/stream/recentchange") as response:
                    async for line in response.content:
                        line = line.decode('utf-8').strip()
                        if not line:
                            continue

                        print(f"Raw line received: {line}")  # Log the raw line for debugging

                        if line.startswith("data:"):
                            json_data = line[len("data:"):].strip()
                            try:
                                message = json.loads(json_data)
                                print(f"Producing message: {message}")  # Print the message for logging
                                await send_to_kafka(producer, message)

                            except json.JSONDecodeError:
                                print(f"Error decoding JSON from line: {json_data}")
                            except Exception as e:
                                print(f"Error processing message: {e}")

                        else:
                            print("Received non-data line. Skipping...")

                # If we successfully read, break out of the retry loop
                break

            except (aiohttp.ClientError, aiohttp.http_exceptions.ClientPayloadError) as e:
                print(f"Error during request: {e}. Retrying in {RETRY_DELAY} seconds...")
                retries += 1
                await asyncio.sleep(RETRY_DELAY)

async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    try:
        await fetch_wikimedia_changes(producer)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
