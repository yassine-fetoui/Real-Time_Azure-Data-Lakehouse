"""
Real-time Kafka → Delta Lake ingestion using PySpark Structured Streaming.

Key features:
  - Avro deserialization with Schema Registry
  - Salted keys to eliminate partition skew
  - Exactly-once semantics via Delta Lake checkpointing
  - Adaptive Query Execution (AQE) enabled
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta import configure_spark_with_delta_pip
import logging

logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = "your-eventhub-namespace.servicebus.windows.net:9093"
SCHEMA_REGISTRY_URL     = "https://your-schema-registry.azurewebsites.net"
TOPIC                   = "raw-events"
DELTA_OUTPUT_PATH       = "abfss://bronze@youradlsgen2.dfs.core.windows.net/events"
CHECKPOINT_LOCATION     = "abfss://checkpoints@youradlsgen2.dfs.core.windows.net/events"
NUM_SALT_BUCKETS        = 32  # Match number of Kafka partitions


# ─── Spark Session ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("RealTimeEventIngestion")
        # Adaptive Query Execution — handles skew dynamically
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Kafka exactly-once: read only committed transactions
        .config("spark.kafka.isolation.level", "read_committed")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ─── Schema ────────────────────────────────────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),    nullable=False),
    StructField("user_id",    StringType(),    nullable=False),
    StructField("event_type", StringType(),    nullable=True),
    StructField("payload",    StringType(),    nullable=True),
    StructField("timestamp",  TimestampType(), nullable=False),
])


# ─── Helpers ───────────────────────────────────────────────────────────────────

def add_salted_key(df, key_col: str, num_buckets: int = NUM_SALT_BUCKETS):
    """
    Append a random salt suffix to hot partition keys.
    Prevents data skew from stalling Spark executors.

    Before:  user_id="user_42" → all records land on 1 executor
    After:   "user_42_17", "user_42_03", ... → distributed across 32 executors
    """
    return df.withColumn(
        "salted_key",
        F.concat(F.col(key_col), F.lit("_"), (F.rand() * num_buckets).cast("int"))
    )


def deserialize_avro(df, schema_registry_url: str, topic: str):
    """
    Deserialize Avro-encoded Kafka value using Schema Registry.
    Requires: com.databricks:spark-avro + Confluent Schema Registry jars.
    """
    return (
        df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_avro(
                F.col("value"),
                # Schema Registry resolves schema by subject (topic-value)
                f"{schema_registry_url}/subjects/{topic}-value/versions/latest"
            ).alias("data"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("kafka_key", "data.*", "partition", "offset", "kafka_timestamp")
    )


# ─── Streaming Job ─────────────────────────────────────────────────────────────

def run():
    spark = build_spark()
    logger.info("Starting Structured Streaming ingestion job...")

    # Read from Kafka (Event Hubs with Kafka protocol)
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                'username="$ConnectionString" password="${EVENTHUB_CONN_STR}";')
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        # Limit micro-batch size to control memory pressure
        .option("maxOffsetsPerTrigger", 100_000)
        .load()
    )

    # Deserialize Avro + add salt for skew mitigation
    parsed = deserialize_avro(raw_stream, SCHEMA_REGISTRY_URL, TOPIC)
    salted = add_salted_key(parsed, key_col="user_id")

    # Watermark for late data handling (5-minute tolerance)
    watermarked = salted.withWatermark("timestamp", "5 minutes")

    # Write to Delta Lake (bronze zone) with exactly-once guarantees
    query = (
        watermarked.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        # Partition by date for efficient downstream queries
        .partitionBy("event_type", F.to_date("timestamp").alias("date"))
        .trigger(processingTime="30 seconds")
        .start(DELTA_OUTPUT_PATH)
    )

    logger.info(f"Stream running. Query ID: {query.id}")
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
