from pyspark import pipelines as dp
import pyspark.sql.types as T
from pyspark.sql.functions import *
from utilities.utils import get_schema

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

# Schema dettagliato della risposta APITube
old_schema = """
STRUCT<
  id: LONG,
  href: STRING,
  published_at: STRING,
  title: STRING,
  description: STRING,
  body: STRING,
  language: STRING,

  author: STRUCT<
    id: LONG,
    name: STRING
  >,

  image: STRING,

  categories: ARRAY<STRUCT<
    id: STRING,
    name: STRING,
    score: DOUBLE,
    taxonomy: STRING,
    links: STRUCT<
      self: STRING
    >
  >>,

  topics: ARRAY<STRING>,

  industries: ARRAY<STRUCT<
    id: LONG,
    name: STRING,
    links: STRUCT<
      self: STRING
    >
  >>,

  entities: ARRAY<STRUCT<
    id: LONG,
    name: STRING,
    links: STRUCT<
      self: STRING,
      wikipedia: STRING,
      wikidata: STRING
    >,
    types: ARRAY<STRING>,
    language: STRING,
    frequency: LONG,
    title: STRUCT<
      pos: ARRAY<STRUCT<
        start: LONG,
        end: LONG
      >>
    >,
    body: STRUCT<
      pos: ARRAY<STRUCT<
        start: LONG,
        end: LONG
      >>
    >
  >>,

  source: STRUCT<
    id: LONG,
    domain: STRING,
    home_page_url: STRING,
    type: STRING,
    bias: STRING,
    rankings: STRUCT<
      opr: DOUBLE
    >,
    location: STRUCT<
      country_name: STRING,
      country_code: STRING
    >,
    favicon: STRING
  >,

  sentiment: STRUCT<
    overall: STRUCT<
      score: DOUBLE,
      polarity: STRING
    >,
    title: STRUCT<
      score: DOUBLE,
      polarity: STRING
    >,
    body: STRUCT<
      score: DOUBLE,
      polarity: STRING
    >
  >,

  summary: ARRAY<STRUCT<
    sentence: STRING,
    sentiment: STRUCT<
      score: DOUBLE,
      polarity: STRING
    >
  >>,

  keywords: ARRAY<STRING>,
  links: ARRAY<STRING>,

  media: ARRAY<STRUCT<
    url: STRING,
    type: STRING,
    format: STRING
  >>,

  story: STRUCT<
    id: LONG,
    uri: STRING
  >,

  is_duplicate: BOOLEAN,
  is_paywall: BOOLEAN,
  is_breaking: BOOLEAN,

  read_time: LONG,
  sentences_count: LONG,
  paragraphs_count: LONG,
  words_count: LONG,
  characters_count: LONG,

  getdata_timestamp: TIMESTAMP,
  enrich_timestamp: TIMESTAMP,
  loaddata_timestamp: TIMESTAMP
>
"""

response_schema = get_schema()

def parse(df):
  return (
    df
    .withColumn("records", col("value").cast("string"))
    .withColumn("parsed_records", from_json(col("records"), old_schema))
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
    "pipelines.reset.allowed": "false"
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