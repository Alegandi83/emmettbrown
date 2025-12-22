from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, StringType
import re

@udf(returnType=BooleanType())
def is_valid_email(email):
    """
    This function checks if the given email address has a valid format using regex.
    Returns True if valid, False otherwise.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None


def get_schema() -> str:
    valid_schema = """
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
    return valid_schema