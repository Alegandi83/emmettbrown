from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr
from pyspark.databricks.sql import functions as dbf

@dp.table(
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "id",
        "delta.enableChangeDataFeed": "true"
    }
)
def clean_news():
    df = (
        spark.readStream.table("dad_open_data.news.eventhub_raw")
        .select(
            "topic", 
            "partition", 
            "offset", 
            "timestamp", 
            "timestampType",
            col("parsed_records.id").alias("id"),
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
        .withWatermark("timestamp", "7 days")
        .dropDuplicates(["id"])
    )



    df = df.withColumn(
        "ai_analysis",
        expr("""
            from_json(
            ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                concat(
                'You are an AI assistant that is focused on reviewing a news article and defining the language it has been written in, a very detailed summary of it based on the title, and which country and city in the world is the most relevant to it. ',
                'You always have to answer a single city, even if not sure about it. You have to return the result in JSON format, with the language, summary, country, city you infer, its actual state_iso3166_2 code (pay attention to obsolete codes), city latitude and longitude and city wikidataID ',
                'The summary and title MUST BE in the same language of the article itself. ',
                'You have to return the result as a JSON, with the following format and without adding absolutely nothing more: { "language": "Italian", "summary": "summary of article", "country": "Italy", "city": "Milan", "state_iso3166_2": "IT-25", "latitude": 123, "longitude": 123, "wikidataID": "Q18502020" }. ',
                'DO NOT return "[json { "language": "Italian", "summary": "detailed summary of article", "country": "Italy", "city": "Milan", "state_iso3166_2": "IT-25", "latitude": 123, "longitude": 123, "wikidataID": "Q18502020" } ]", only the actual JSON. This is the source, composed by a title and a description, where apply your analysis: --- TITLE ---',
                parsed_records.title,
                ' --- DESCRIPTION ---',
                parsed_records.description
                ),
                responseFormat => '{
                "type": "json_schema",
                "json_schema": {
                    "name": "news_ai_analysis",
                    "schema": {
                    "type": "object",
                    "properties": {
                        "language":         { "type": "string" },
                        "summary":          { "type": "string" },
                        "country":          { "type": "string" },
                        "city":             { "type": "string" },
                        "state_iso3166_2":  { "type": "string" },
                        "latitude":         { "type": "number" },
                        "longitude":        { "type": "number" },
                        "wikidataID":       { "type": "string" }
                    },
                    "required": ["language", "summary", "country", "city", "state_iso3166_2", "latitude", "longitude", "wikidataID"],
                    "additionalProperties": false
                    }
                }
                }'
            ),
            'STRUCT<
                language        STRING,
                summary         STRING,
                country         STRING,
                city            STRING,
                state_iso3166_2 STRING,
                latitude        DOUBLE,
                longitude       DOUBLE,
                wikidataID      STRING
            >'
            )
        """)
    ).withColumn("ai_language", col("ai_analysis.language")) \
    .withColumn("ai_summary",   col("ai_analysis.summary"))  \
    .withColumn("ai_country",   col("ai_analysis.country"))  \
    .withColumn("ai_city",      col("ai_analysis.city"))     \
    .withColumn("ai_state",     col("ai_analysis.state_iso3166_2")) \
    .withColumn("ai_latitude",  col("ai_analysis.latitude")) \
    .withColumn("ai_longitude", col("ai_analysis.longitude")) \
    .withColumn("ai_wikidataID",col("ai_analysis.wikidataID")) \
    .withColumn("ai_h3_res_15", dbf.h3_longlatash3(col("ai_analysis.longitude"), col("ai_analysis.latitude"), expr("15")))
    
    return df