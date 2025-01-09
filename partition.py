from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DebugForeach").getOrCreate()

# Sample DataFrame
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
df = spark.createDataFrame(data, ["id", "name"])

# Debugging with foreachPartition
def process_partition(partition):
    for row in partition:
        print(f"Row: {row}")

df.foreachPartition(process_partition)

# Optional: Collect data to driver for debugging
print("Data collected to driver for debugging:")
for row in df.collect():
    print(row)
