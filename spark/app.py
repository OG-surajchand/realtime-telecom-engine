import redis
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("order_id",  StringType()),
    StructField("user_id",   StringType()),
    StructField("amount",    DoubleType()),
    StructField("status",    StringType())
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29092,kafka-3:29092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING)")
    .select(F.from_json("value", schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", F.to_timestamp("timestamp"))
)

def write_batch(batch_df, batch_id):
    print(f"--- Processing Batch ID: {batch_id} ---")
    batch_df.show()

    if batch_df.isEmpty():
        return

    # ── 1. Persist raw rows to Postgres ───────────────────────────────────
    jdbc_url = "jdbc:postgresql://postgres:5432/iot_db"
    conn_props = {
        "user":     "user",
        "password": "password",
        "driver":   "org.postgresql.Driver",
    }
    batch_df.write.jdbc(
        url=jdbc_url,
        table="orders",
        mode="append",
        properties=conn_props,
    )

    # ── 2. Compute max amount per user_id inside this micro-batch ─────────
    max_per_user = (
        batch_df
        .groupBy("user_id")
        .agg(F.max("amount").alias("batch_max"))
        .collect()                          # driver-side: lightweight aggregation
    )

    r = redis.Redis(host="redis", port=6379, decode_responses=True)

    pipe = r.pipeline()
    for row in max_per_user:
        user_id   = row["user_id"]
        batch_max = float(row["batch_max"])

        pipe.zadd("orders:max_amount", {user_id: batch_max}, gt=True)

        redis_key = f"orders:max_amount:{user_id}"
        lua_script = """
            local current = redis.call('GET', KEYS[1])
            if current == false or tonumber(ARGV[1]) > tonumber(current) then
                redis.call('SET', KEYS[1], ARGV[1])
                return 1
            end
            return 0
        """
        pipe.eval(lua_script, 1, redis_key, batch_max)

    pipe.execute()
    print(f"[Batch {batch_id}] Redis updated for {len(max_per_user)} user(s).")

query = (
    df_parsed.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", "/tmp/spark_checkpoints/kafka_to_postgres")
    .start()
)

query.awaitTermination()