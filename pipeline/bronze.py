
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
)


# Estrutura dos produtos associados a um evento.
ITEM_SCHEMA = StructType([
    StructField("index", LongType(), True),
    StructField("item_category", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("item_list_id", StringType(), True),
    StructField("item_list_name", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", LongType(), True),
])


# Campos específicos de cada tipo de evento.
PARAMS_SCHEMA = StructType([
    StructField("coupon", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("element_id", StringType(), True),
    StructField("file_extension", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("item_list_id", StringType(), True),
    StructField("item_list_name", StringType(), True),
    StructField("lead_source", StringType(), True),
    StructField("link_domain", StringType(), True),
    StructField("link_id", StringType(), True),
    StructField("link_text", StringType(), True),
    StructField("link_url", StringType(), True),
    StructField("method", StringType(), True),
    StructField("outbound", BooleanType(), True),
    StructField("page_title", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("percent_scrolled", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("search_area", StringType(), True),
    StructField("search_results", LongType(), True),
    StructField("search_term", StringType(), True),
    StructField("shipping", DoubleType(), True),
    StructField("shipping_tier", StringType(), True),
    StructField("tax", DoubleType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("video_current_time", LongType(), True),
    StructField("video_duration", LongType(), True),
    StructField("video_percent", LongType(), True),
    StructField("video_provider", StringType(), True),
    StructField("video_title", StringType(), True),
])


# Schema completo do evento recebido na Landing.
EVENT_SCHEMA = StructType([
    StructField("device", StructType([
        StructField("browser", StringType(), True),
        StructField("category", StringType(), True),
        StructField("os", StringType(), True),
    ]), True),

    StructField("event_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_ts", StringType(), True),

    StructField("items", ArrayType(ITEM_SCHEMA), True),

    StructField("page", StructType([
        StructField("location", StringType(), True),
        StructField("referrer", StringType(), True),
    ]), True),

    StructField("params", PARAMS_SCHEMA, True),
    StructField("session_hint", StringType(), True),

    StructField("traffic", StructType([
        StructField("campaign", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("source", StringType(), True),
    ]), True),

    StructField("user_id", StringType(), True),
    StructField("user_pseudo_id", StringType(), True),
])

from comum import (
    criar_sessao,
    exigir_camada,
    glob_eventos,
    gravar_eventos,
    ler_eventos,
    titulo,
)

from pyspark.sql import functions as F

spark = criar_sessao("bronze")

# Verifica se existem dados na Landing.
exigir_camada(
    spark,
    glob_eventos("landing"),
    "python gerador/gerar_eventos.py",
)

# Lê os eventos usando nosso schema explícito.
landing = ler_eventos(
    spark,
    "landing",
    "json",
    schema=EVENT_SCHEMA,
)

# Registra a origem de cada evento e o instante da ingestão.
bronze = landing.withColumn(
    "arquivo_origem",
    F.input_file_name(),
).withColumn(
    "instante_ingestao",
    F.current_timestamp(),
)

# Garante que as colunas de partição sejam textos.
bronze = (
    bronze
    .withColumn("event_name", F.col("event_name").cast("string"))
    .withColumn("year", F.col("year").cast("string"))
    .withColumn("month", F.col("month").cast("string"))
    .withColumn("day", F.col("day").cast("string"))
)

# Grava os eventos na camada Bronze em formato Parquet.
gravar_eventos(bronze, "bronze", "append")

# Exibe uma mensagem de conclusão.
titulo("Bronze concluída")

# Encerra a sessão Spark.
spark.stop()