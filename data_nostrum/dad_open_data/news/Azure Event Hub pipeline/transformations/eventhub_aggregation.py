from pyspark import pipelines as dp
from pyspark.sql.functions import col

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

@dp.table
def eventhub_clean():
    return (
        spark.read.table("dad_open_data.news.eventhub_raw")
        .select(
                "topic", 
                "partition", 
                "offset", 
                "timestamp", 
                "timestampType",
                "parsed_records.id",
                "parsed_records.href",
                "parsed_records.published_at",
                "parsed_records.title",
                "parsed_records.description",
                "parsed_records.body",
                "parsed_records.language",
                col("parsed_records.author.id").alias("author_id"),
                "parsed_records.author.name",
                "parsed_records.image",
                "parsed_records.categories",
                "parsed_records.topics",
                "parsed_records.industries",
                "parsed_records.entities",
                "parsed_records.source",
                "parsed_records.sentiment",
                "parsed_records.summary",
                "parsed_records.keywords",
                "parsed_records.links",
                "parsed_records.media",
                "parsed_records.story",
                "parsed_records.is_duplicate",
                "parsed_records.is_paywall",
                "parsed_records.is_breaking",
                "parsed_records.read_time",
                "parsed_records.sentences_count",
                "parsed_records.paragraphs_count",
                "parsed_records.words_count",
                "parsed_records.characters_count",
                "parsed_records.getdata_timestamp",
                "parsed_records.enrich_timestamp",
                "parsed_records.loaddata_timestamp",
                "parsed_records",
                "eh_enqueued_timestamp",
                "eh_enqueued_date",
                "etl_processed_timestamp",
                "etl_rec_uuid"
            )
    )