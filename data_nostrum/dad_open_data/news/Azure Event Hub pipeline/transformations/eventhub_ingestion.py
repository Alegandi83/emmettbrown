from pyspark import pipelines as dp
import pyspark.sql.types as T
from pyspark.sql.functions import *

# Event Hubs configuration
EH_NAMESPACE                    = spark.conf.get("iot.ingestion.eh.namespace")
EH_NAME                         = spark.conf.get("iot.ingestion.eh.name")
EH_CONN_SHARED_ACCESS_KEY_NAME  = spark.conf.get("iot.ingestion.eh.accessKeyName")

# Event Hubs configuration - key
SECRET_SCOPE                    = spark.conf.get("io.ingestion.eh.secretsScopeName")
SECRET_NAME                     = spark.conf.get("io.ingestion.eh.secretName")
EH_CONN_SHARED_ACCESS_KEY_VALUE = dbutils.secrets.get(scope = f"{SECRET_SCOPE}", key = f"{SECRET_NAME}")

# Event Hubs configuration - connectionString
EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"
# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : spark.conf.get("iot.ingestion.kafka.requestTimeout"),
  "kafka.session.timeout.ms" : spark.conf.get("iot.ingestion.kafka.sessionTimeout"),
  "maxOffsetsPerTrigger"     : spark.conf.get("iot.ingestion.spark.maxOffsetsPerTrigger"),
  "failOnDataLoss"           : spark.conf.get("iot.ingestion.spark.failOnDataLoss"),
  "startingOffsets"          : spark.conf.get("iot.ingestion.spark.startingOffsets")
}


# Basic record parsing and adding ETL audit columns
def parse(df):
  return (df
    .withColumn("records", col("value").cast("string"))
    .withColumn("parsed_records", col("records"))
    .withColumn("eh_enqueued_timestamp", expr("timestamp"))
    .withColumn("eh_enqueued_date", expr("to_date(timestamp)"))
    .withColumn("etl_processed_timestamp", col("current_timestamp"))
    .withColumn("etl_rec_uuid", expr("uuid()"))
    .drop("records", "value", "key")
  )

@dp.create_table(
  comment="Raw EventHub Events",
  table_properties={
    "quality": "bronze",
    "pipelines.reset.allowed": "false" # preserves the data in the delta table if you do full refresh
  },
  partition_cols=["eh_enqueued_date"]
)
@dp.expect("valid_topic", "topic IS NOT NULL")
@dp.expect("valid records", "parsed_records IS NOT NULL")
def eventhub_raw():
  return (
   spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
    .transform(parse)
  )