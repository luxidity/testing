from pyspark.sql import SparkSession
import requests
from tenacity import retry, wait_exponential, stop_after_attempt
import time

# Configuration
BATCH_SIZE = 30
API_URL = "https://example.com/api/endpoint"
MAX_REQUESTS_PER_SECOND = 800  # Adjusted for 10 workers
MAX_RETRIES = 5

# Retry logic
@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(MAX_RETRIES))
def send_batch(batch):
    response = requests.post(API_URL, json=batch)
    if response.status_code != 200:
        raise Exception(f"Request failed with status {response.status_code}")
    return response.json()

# Process each partition
def process_partition(partition):
    start_time = time.time()
    batch = []
    request_count = 0

    for row in partition:
        batch.append(row.asDict())
        if len(batch) == BATCH_SIZE:
            # Send the batch
            send_batch(batch)
            batch = []
            request_count += 1

            # Rate limit logic
            elapsed_time = time.time() - start_time
            if request_count >= MAX_REQUESTS_PER_SECOND:
                sleep_time = max(0, 1 - elapsed_time)
                time.sleep(sleep_time)
                start_time = time.time()
                request_count = 0

    # Send any remaining records
    if batch:
        send_batch(batch)

# PySpark setup
spark = SparkSession.builder.appName("GlobalRateLimitAPIRequests").getOrCreate()

# Load your DataFrame
df = spark.read.csv("your_data.csv", header=True)

# Repartition to limit the number of workers (Optional for Option C)
df = df.repartition(10)  # Adjust number of workers to match your needs

# Process partitions
df.foreachPartition(process_partition)
