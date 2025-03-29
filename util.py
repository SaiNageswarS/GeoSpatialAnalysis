import asyncio
import random
from temporalio.client import Client


async def connect_with_backoff(address, max_retries=8, base_delay=1, max_delay=30):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempt {attempt} connecting to Temporal at {address}...")
            return await Client.connect(address)
        except Exception as e:
            wait_time = min(base_delay * (2 ** (attempt - 1)), max_delay)
            # Add a small jitter to avoid thundering herd
            jitter = random.uniform(0, 0.5)
            total_wait = wait_time + jitter
            print(f"Connection failed: {e}. Retrying in {total_wait:.2f} seconds...")
            await asyncio.sleep(total_wait)
    raise RuntimeError(f"Could not connect to Temporal at {address} after {max_retries} retries")
