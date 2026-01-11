from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, struct, to_json, monotonically_increasing_id
from pyspark.databricks.sql import functions as dbf

@dp.table
def survey_ai():
    """
    Delta Live Table che crea la tabella survey_ai nel namespace DLT.
    Genera automaticamente sondaggi basati sulle notizie analizzate.
    
    FLUSSO:
    1. Legge da dad_open_data.news.eventhub_clean_ai
    2. Applica un prompt AI per creare sondaggi strutturati
    3. Usa il campo 'id' da eventhub_clean_ai (relazione 1:1) come BIGINT
    4. Scrive nella tabella DLT LIVE.survey_ai
    5. La tabella survey_options_ai leggerà da LIVE.survey_ai usando il campo 'id'
    
    NOTA: 
    - Il campo 'id' è di tipo BIGINT preso direttamente da eventhub_clean_ai
    - La relazione è 1:1: ogni record di eventhub_clean_ai genera un survey con lo stesso id
    - La tabella viene creata nel namespace DLT (LIVE.survey_ai) perché una tabella MANAGED 
      esiste già in dad_open_data.news.survey_ai
    """
    
    # Leggi dalla tabella esistente eventhub_clean_ai
    df = spark.readStream.table("dad_open_data.news.eventhub_clean_ai")
    
    # Prepara l'input JSON per l'AI usando i campi già analizzati
    df = df.withColumn(
        "news_input_json",
        to_json(struct(
            col("ai_language").alias("language"),
            col("ai_summary").alias("summary"),
            col("ai_country").alias("country"),
            col("ai_city").alias("city"),
            col("ai_state").alias("ai_state"),
            col("ai_latitude").alias("latitude"),
            col("ai_longitude").alias("longitude"),
            col("ai_wikidataID").alias("wikidataID")
        ))
    )
    
    # Applica il prompt per la creazione del sondaggio
    df = df.withColumn(
        "survey_generation",
        expr("""
            from_json(
            ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                concat(
                        '# AGENT PROMPT: Survey Creator from News\\n\\n',
                        'You are an expert news analyst and survey creator for the Web Democracy platform. ',
                        'Analyze the provided news and create a relevant survey that stimulates democratic debate.\\n\\n',
                        
                        '## IMPORTANTE - LINGUA ITALIANA:\\n',
                        '⚠️ Il sondaggio DEVE essere SEMPRE in ITALIANO, indipendentemente dalla lingua della notizia originale.\\n',
                        '- survey.title: SEMPRE in italiano\\n',
                        '- survey.description: SEMPRE in italiano\\n',
                        '- options[].option_text: SEMPRE in italiano\\n',
                        '- Anche se la notizia è in inglese, francese, spagnolo, ecc., traduci tutto in italiano.\\n\\n',
                        
                        '## INPUT NEWS ANALYSIS:\\n',
                        news_input_json,
                        '\\n\\n',
                        
                        '## YOUR TASK:\\n',
                        '1. Analyze the summary to identify the main theme, sentiment, and implicit questions\\n',
                        '2. Choose the most appropriate question type (single_choice, multiple_choice, rating, scale, open_text, date)\\n',
                        '3. Determine survey characteristics:\\n',
                        '   - closure_type: "permanent" (evergreen topics), "scheduled" (time-sensitive, +7-30 days), "manual" (moderator decides)\\n',
                        '   - show_results_on_close: true (sensitive topics), false (informal surveys)\\n',
                        '   - allow_custom_options: true (open themes), false (structured questions)\\n',
                        '   - is_anonymous: true (politically sensitive), false (transparency required)\\n',
                        '   - rating_icon: "star" (generic), "heart" (appreciation), "number" (technical)\\n',
                        '4. Generate 3-5 balanced options IN ITALIAN (max 60 characters each)\\n',
                        '   ⚠️ For ALL question types (including open_text): ALWAYS provide options\\n',
                        '   - For open_text: options are THEMES/CATEGORIES users can comment on\\n',
                        '   - Example open_text options: "Aspetti positivi", "Criticità", "Proposte"\\n',
                        '5. Create a clear title IN ITALIAN (max 60 characters) and description with context\\n\\n',
                        
                        '## QUESTION TYPE SELECTION RULES:\\n',
                        '- single_choice: Clear position needed (agree/disagree, favor/oppose)\\n',
                        '- multiple_choice: Complex topics with multiple valid answers\\n',
                        '- rating: Evaluate services, performances, proposals (with rating_icon)\\n',
                        '- scale: Measure opinion intensity (1-10) with min/max labels\\n',
                        '- open_text: Qualitative feedback, creative ideas. ⚠️ IMPORTANT: Still provide 3-5 options as THEMES/CATEGORIES where users can add free text comments\\n',
                        '- date: Event planning, shared scheduling\\n\\n',
                        
                        '## OUTPUT REQUIREMENTS:\\n',
                        '- ⚠️ CRITICO: Title, description e option_text DEVONO essere in ITALIANO (non nella lingua della notizia)\\n',
                        '- ⚠️ LIMITI LUNGHEZZA: Title max 60 caratteri, option_text max 60 caratteri\\n',
                        '- ⚠️ OPTIONS REQUIRED: ALWAYS provide 3-5 options, even for open_text surveys\\n',
                        '  * For single_choice/multiple_choice: specific answer choices\\n',
                        '  * For rating/scale: aspects to evaluate\\n',
                        '  * For open_text: themes/categories for user comments (es: "Aspetti positivi", "Criticità")\\n',
                        '  * For date: proposed date options\\n',
                        '- Se la notizia è in altra lingua, TRADUCI il contenuto del sondaggio in italiano\\n',
                        '- Title: conciso, diretto, interrogativo (es: "Sei favorevole alla nuova legge?")\\n',
                        '- Options: brevi, chiare, bilanciate (es: "Sì, favorevole", "No, contrario")\\n',
                        '- Balanced options without bias\\n',
                        '- Geographic context (city/country) when relevant\\n',
                        '- Neutral and inclusive language\\n',
                        '- resource_type must be "news"\\n',
                        '- Provide detailed reasoning for your choices\\n\\n',
                        
                        '## ESEMPI:\\n',
                        '### Example 1 - single_choice:\\n',
                        'Input: {"language": "en", "summary": "The EU approves new climate regulations..."}\\n',
                        'Output title: "Favorevole alle nuove norme UE sul clima?" (60 chars)\\n',
                        'Output options: ["Sì, necessarie", "No, troppo rigide", "Indeciso"] (60 chars max)\\n\\n',
                        '### Example 2 - open_text:\\n',
                        'Input: {"summary": "Governo lancia piano mobilità sostenibile..."}\\n',
                        'Output title: "Cosa pensi del piano mobilità sostenibile?" (60 chars)\\n',
                        'Output options: ["Aspetti positivi", "Criticità", "Proposte migliorative"] (themes for comments)\\n\\n',
                        
                        'Generate the survey following the JSON schema exactly.\\n',
                        '⚠️ REMEMBER: \\n',
                        '1. All text fields MUST be in Italian\\n',
                        '2. Title MUST be max 60 characters\\n',
                        '3. Each option_text MUST be max 60 characters\\n',
                        '4. ALWAYS provide 3-5 options (even for open_text surveys as themes/categories)\\n',
                        '5. Be concise and direct'
                ),
                responseFormat => '{
                "type": "json_schema",
                "json_schema": {
                            "name": "survey_generation",
                    "schema": {
                    "type": "object",
                    "properties": {
                                    "survey": {
                                        "type": "object",
                                        "properties": {
                                            "title": {"type": "string", "maxLength": 60},
                                            "description": {"type": "string"},
                                            "question_type": {"type": "string", "enum": ["single_choice", "multiple_choice", "open_text", "scale", "rating", "date"]},
                                            "closure_type": {"type": "string", "enum": ["permanent", "scheduled", "manual"]},
                                            "expires_at": {"type": ["string", "null"]},
                                            "is_active": {"type": "boolean"},
                                            "show_results_on_close": {"type": "boolean"},
                                            "allow_multiple_responses": {"type": "boolean"},
                                            "allow_custom_options": {"type": "boolean"},
                                            "require_comment": {"type": "boolean"},
                                            "rating_icon": {"type": ["string", "null"], "enum": ["star", "heart", "number", null]},
                                            "is_anonymous": {"type": "boolean"},
                                            "resource_type": {"type": "string", "enum": ["news"]},
                                            "resource_url": {"type": "string"},
                                            "min_value": {"type": ["integer", "null"]},
                                            "max_value": {"type": ["integer", "null"]},
                                            "scale_min_label": {"type": ["string", "null"]},
                                            "scale_max_label": {"type": ["string", "null"]}
                                        },
                                        "required": ["title", "description", "question_type", "closure_type", "is_active", "show_results_on_close", "allow_custom_options", "is_anonymous", "resource_type"]
                                    },
                                    "options": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "option_text": {"type": "string", "maxLength": 60},
                                                "option_order": {"type": "integer"}
                                            },
                                            "required": ["option_text", "option_order"]
                                        }
                                    },
                                    "metadata": {
                                        "type": "object",
                                        "properties": {
                                            "geographic_context": {
                                                "type": "object",
                                                "properties": {
                                                    "country": {"type": "string"},
                                                    "city": {"type": "string"},
                                                    "state": {"type": "string"},
                                                    "latitude": {"type": "number"},
                                                    "longitude": {"type": "number"},
                                                    "wikidataID": {"type": "string"}
                                                }
                                            },
                                            "language": {"type": "string"}
                                        }
                                    },
                                    "reasoning": {
                                        "type": "object",
                                        "properties": {
                                            "question_type_rationale": {"type": "string"},
                                            "closure_type_rationale": {"type": "string"},
                                            "anonymity_rationale": {"type": "string"},
                                            "options_rationale": {"type": "string"},
                                            "geographic_relevance": {"type": "string"}
                                        }
                                    }
                                },
                                "required": ["survey", "options", "metadata", "reasoning"]
                    }
                }
                }'
            ),
            'STRUCT<
                    survey STRUCT<
                        title STRING,
                        description STRING,
                        question_type STRING,
                        closure_type STRING,
                        expires_at STRING,
                        is_active BOOLEAN,
                        show_results_on_close BOOLEAN,
                        allow_multiple_responses BOOLEAN,
                        allow_custom_options BOOLEAN,
                        require_comment BOOLEAN,
                        rating_icon STRING,
                        is_anonymous BOOLEAN,
                        resource_type STRING,
                        resource_url STRING,
                        min_value INT,
                        max_value INT,
                        scale_min_label STRING,
                        scale_max_label STRING
                    >,
                    options ARRAY<STRUCT<
                        option_text STRING,
                        option_order INT
                    >>,
                    metadata STRUCT<
                        geographic_context STRUCT<
                            country STRING,
                            city STRING,
                            state STRING,
                            latitude DOUBLE,
                longitude DOUBLE,
                wikidataID STRING
                        >,
                        language STRING
                    >,
                    reasoning STRUCT<
                        question_type_rationale STRING,
                        closure_type_rationale STRING,
                        anonymity_rationale STRING,
                        options_rationale STRING,
                        geographic_relevance STRING
                    >
            >'
            )
        """)
    )
    
    # Esplodi i campi del sondaggio generato per facilitare l'accesso (senza prefisso survey_)
    # Il campo 'id' viene preso direttamente da eventhub_clean_ai (relazione 1:1) come BIGINT
    df = df \
        .withColumn("id", col("id")) \
        .withColumn("title", col("survey_generation.survey.title")) \
        .withColumn("description", col("survey_generation.survey.description")) \
        .withColumn("question_type", col("survey_generation.survey.question_type")) \
        .withColumn("closure_type", col("survey_generation.survey.closure_type")) \
        .withColumn("expires_at", col("survey_generation.survey.expires_at")) \
        .withColumn("is_active", col("survey_generation.survey.is_active")) \
        .withColumn("show_results_on_close", col("survey_generation.survey.show_results_on_close")) \
        .withColumn("allow_multiple_responses", expr("false")) \
        .withColumn("allow_custom_options", col("survey_generation.survey.allow_custom_options")) \
        .withColumn("require_comment", expr("false")) \
        .withColumn("is_anonymous", col("survey_generation.survey.is_anonymous")) \
        .withColumn("resource_type", col("survey_generation.survey.resource_type")) \
        .withColumn("resource_url", col("href")) \
        .withColumn("resource_news_id", col("id")) \
        .withColumn("user_id", expr("1")) \
        .withColumn("rating_icon", col("survey_generation.survey.rating_icon")) \
        .withColumn("min_value", col("survey_generation.survey.min_value")) \
        .withColumn("max_value", col("survey_generation.survey.max_value")) \
        .withColumn("scale_min_label", col("survey_generation.survey.scale_min_label")) \
        .withColumn("scale_max_label", col("survey_generation.survey.scale_max_label")) \
        .withColumn("created_at", expr("current_timestamp()")) \
        .withColumn("options", col("survey_generation.options")) \
        .withColumn("metadata", col("survey_generation.metadata")) \
        .withColumn("reasoning", col("survey_generation.reasoning")) \
        .withColumn("generated_at", expr("current_timestamp()")) \
        .withColumn("survey_generation_json", col("survey_generation"))

    
    # Seleziona solo i campi del sondaggio generato da scrivere nella tabella
    df = df.select(

        # Sondaggio generato (campi principali)
        "id",      # Primary Key
        "title",
        "description",
        "question_type",
        "min_value",
        "max_value",
        "scale_min_label",
        "scale_max_label",
        "created_at",
        "closure_type",
        "expires_at",
        "is_active",
        "show_results_on_close",
        "allow_multiple_responses",
        "allow_custom_options",
        "require_comment",
        "rating_icon",
        "is_anonymous",
        "resource_type",
        "resource_url",
        "resource_news_id",
        "user_id",
        
        # Sondaggio generato (strutture complesse)
        "options",
        "metadata",
        "reasoning",
        "generated_at",
        
        # JSON completo per riferimento
        "survey_generation_json",
        "news_input_json"
    )
    
    return df


@dp.table
def survey_options_ai():

    """
    Delta Live Table che crea la tabella survey_options_ai nel namespace DLT.
    
    Legge dalla tabella DLT LIVE.survey_ai che contiene il dataframe già elaborato.
    
    FLUSSO:
    1. Legge dalla tabella DLT LIVE.survey_ai (dataframe della funzione survey_ai)
    2. Esplode l'array 'options' per creare una riga per ogni opzione
    3. Genera un ID BIGINT per ogni opzione concatenando survey_id + option_order
    4. Usa 'id' da LIVE.survey_ai come 'survey_id' per il collegamento
    5. Scrive nella tabella DLT LIVE.survey_options_ai
    
    NOTA: 
    - Il campo 'id' è di tipo BIGINT generato concatenando survey_id (da eventhub_clean_ai) con option_order
    - La generazione è deterministica: stesso survey_id + option_order → stesso id
    - Esempio: survey_id=123, option_order=1 → id=1231
    - La tabella viene creata nel namespace DLT (LIVE.survey_options_ai) perché una tabella MANAGED 
      esiste già in dad_open_data.news.survey_options_ai
    """
    
    # Leggi dalla tabella DLT survey_ai (usa il dataframe df già elaborato)
    df_options = spark.readStream.table("LIVE.survey_ai")
    
    # Importa le funzioni necessarie
    from pyspark.sql.functions import explode
    
    # Esplodi l'array di opzioni per creare una riga per ogni opzione
    # Il campo 'id' è disponibile dalla tabella survey_ai (derivato da eventhub_clean_ai)
    df_options = df_options.select(
        col("id").alias("survey_id"),  # Campo BIGINT da eventhub_clean_ai (relazione 1:1)
        col("created_at").alias("survey_created_at"),
        explode("options").alias("option")
    )
    
    # Genera ID per questa tabella e estrai i campi dall'opzione
    # L'ID è derivato concatenando survey_id con option_order come BIGINT
    df_options = df_options \
        .withColumn("option_text", col("option.option_text")) \
        .withColumn("option_order", col("option.option_order")) \
        .withColumn("id", expr("cast(concat(cast(survey_id as string), cast(option.option_order as string)) as bigint)")) \
        .withColumn("created_at", col("survey_created_at")) \
        .withColumn("user_id", expr("1"))
    
    # Seleziona i campi finali da scrivere
    df_options = df_options.select(
        "id",              # ID BIGINT generato concatenando survey_id + option_order
        "survey_id",       # ID del survey (da eventhub_clean_ai tramite survey_ai)
        "option_text",     # Testo dell'opzione
        "option_order",    # Ordine di visualizzazione
        "created_at",      # Timestamp ereditato dal survey
        "user_id"          # User ID (default: 1 per AI)
    )
    
    return df_options
