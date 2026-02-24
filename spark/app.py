from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .getOrCreate()

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29092,kafka-3:29092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType())
])

# 2. Parse the JSON payload
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json("value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", F.to_timestamp("timestamp"))

def write_to_postgres(batch_df, batch_id):
    print(f"--- Processing Batch ID: {batch_id} ---")
    batch_df.show() 
    
    jdbc_url = "jdbc:postgresql://postgres:5432/iot_db"
    connection_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    
    if not batch_df.isEmpty():
        batch_df.write.jdbc(
            url=jdbc_url, 
            table="orders", 
            mode="append", 
            properties=connection_properties
        )

query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/kafka_to_postgres") \
    .start()

query.awaitTermination()