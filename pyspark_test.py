from pyspark.sql import SparkSession
import requests
import time
from tenacity import retry, wait_exponential, stop_after_attempt

# Configuration
BATCH_SIZE = 30
API_URL = "https://example.com/api/endpoint"
MAX_REQUESTS_PER_SECOND = 800  # Adjust based on global limit and worker count
MAX_RETRIES = 5

# Retry logic for HTTP requests
@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(MAX_RETRIES))
def send_batch(batch):
    response = requests.post(API_URL, json=batch)
    if response.status_code != 200:
        raise Exception(f"Request failed with status {response.status_code}")
    return response.json()

# Process each partition with rate limiting
def process_partition(partition):
    start_time = time.time()
    batch, request_count = [], 0

    for row in partition:
        batch.append(row.asDict())
        if len(batch) == BATCH_SIZE:
            send_batch(batch)
            batch, request_count = [], request_count + 1

            # Rate limiting
            if request_count >= MAX_REQUESTS_PER_SECOND:
                time.sleep(max(0, 1 - (time.time() - start_time)))
                start_time, request_count = time.time(), 0

    if batch:
        send_batch(batch)  # Send remaining records

# Spark setup
spark = SparkSession.builder.appName("SimplifiedBatchAPI").getOrCreate()
df = spark.read.csv("your_data.csv", header=True)

# Adjust partition count if needed (e.g., to control workers)
df = df.repartition(10)

# Process partitions
df.foreachPartition(process_partition)
