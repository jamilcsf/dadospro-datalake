
"""Camada Bronze: ingestão dos eventos da Landing em Parquet."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
    MapType,
)

from comum import (
    criar_sessao,
    exigir_camada,
    glob_eventos,
    gravar_eventos,
    ler_eventos,
    titulo,
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


# Preserva os parâmetros recebidos, sem restringir suas chaves.
# A conversão para os tipos analíticos acontece na Silver.
PARAMS_SCHEMA = MapType(
    StringType(),
    StringType(),
    valueContainsNull=True,
)


# Schema explícito dos eventos recebidos na Landing.
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


def ler_landing(spark: SparkSession) -> DataFrame:
    """Lê os eventos da Landing diretamente pelo S3A."""

    return ler_eventos(
        spark,
        "landing",
        "json",
        schema=EVENT_SCHEMA,
    )


def adicionar_proveniencia(eventos: DataFrame) -> DataFrame:
    """Registra a origem, a ingestão e a data de coleta."""

    bronze = (
        eventos
        .withColumn(
            "arquivo_origem",
            F.input_file_name(),
        )
        .withColumn(
            "instante_ingestao",
            F.current_timestamp(),
        )
    )

    # A data de coleta vem do caminho da Landing,
    # e não do timestamp informado pelo navegador.
    bronze = (
        bronze
        .withColumn(
            "year",
            F.regexp_extract(
                F.col("arquivo_origem"),
                r"/year=(\d{4})/",
                1,
            ),
        )
        .withColumn(
            "month",
            F.regexp_extract(
                F.col("arquivo_origem"),
                r"/month=(\d{2})/",
                1,
            ),
        )
        .withColumn(
            "day",
            F.regexp_extract(
                F.col("arquivo_origem"),
                r"/day=(\d{2})/",
                1,
            ),
        )
        .withColumn(
            "event_name",
            F.col("event_name").cast("string"),
        )
    )

    return bronze


def main() -> None:
    """Executa o job de ingestão da camada Bronze."""

    spark = criar_sessao("bronze")
    bronze = None

    try:
        # Verifica a existência da camada de origem.
        exigir_camada(
            spark,
            glob_eventos("landing"),
            "docker compose run --rm app python gerador/gerar_eventos.py",
        )

        titulo("Ingestão da camada Bronze")

        landing = ler_landing(spark)

        bronze = adicionar_proveniencia(landing).cache()

        total_eventos = bronze.count()

        total_arquivos = (
            bronze
            .select("arquivo_origem")
            .distinct()
            .count()
        )

        print(f"Eventos recebidos: {total_eventos}")
        print(f"Arquivos de origem: {total_arquivos}")

        # Append-only: preserva as ingestões anteriores.
        # A deduplicação será realizada na camada Silver.
        gravar_eventos(
            bronze,
            "bronze",
            "append",
        )

        total_acumulado = ler_eventos(
            spark,
            "bronze",
            "parquet",
        ).count()

        print(f"Eventos acumulados na Bronze: {total_acumulado}")

        titulo("Bronze concluída")

    finally:
        if bronze is not None:
            bronze.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
