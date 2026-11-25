
from pyspark.sql import functions as F

from comum import criar_sessao, caminho, ler_eventos, titulo


# Inicia a sessão Spark.
spark = criar_sessao("gold_sessoes")

# Lê os eventos válidos da Silver.
silver = ler_eventos(spark, "silver")

# Define a ordenação para identificar o primeiro evento.
ordem_evento = F.struct(
    F.col("event_ts"),
    F.col("event_id"),
)

# Consolida todos os eventos em uma linha por sessão.
sessoes = (
    silver
    .groupBy("session_id")
    .agg(
        F.first("visitante_id").alias("visitante_id"),
        F.min("event_ts").alias("inicio"),
        F.max("event_ts").alias("fim"),
        F.count("*").alias("total_eventos"),
        F.max("params.percent_scrolled").alias("scroll_maximo"),
        F.max("params.video_percent").alias("video_percent_maximo"),
        F.max(
            F.when(F.col("event_name") == "generate_lead", 1)
            .otherwise(0)
        ).cast("boolean").alias("fez_lead"),
        F.max(
            F.when(F.col("event_name") == "purchase", 1)
            .otherwise(0)
        ).cast("boolean").alias("comprou"),
        F.min_by(
            F.struct(
                F.col("traffic.source").alias("source"),
                F.col("traffic.medium").alias("medium"),
                F.col("device.category").alias("categoria"),
                F.col("device.browser").alias("navegador"),
                F.col("device.os").alias("sistema_operacional"),
            ),
            ordem_evento,
        ).alias("primeiro_evento"),
    )
)

# Calcula a duração e organiza os campos.
sessoes = (
    sessoes
    .withColumn(
        "duracao_segundos",
        F.col("fim").cast("long") - F.col("inicio").cast("long"),
    )
    .withColumn(
        "canal",
        F.concat_ws(
            " / ",
            F.coalesce(F.col("primeiro_evento.source"), F.lit("(direct)")),
            F.coalesce(F.col("primeiro_evento.medium"), F.lit("(none)")),
        ),
    )
    .withColumn(
        "dispositivo",
        F.col("primeiro_evento.categoria"),
    )
    .withColumn(
        "navegador",
        F.col("primeiro_evento.navegador"),
    )
    .withColumn(
        "sistema_operacional",
        F.col("primeiro_evento.sistema_operacional"),
    )
    .drop("primeiro_evento")
)

# Grava a tabela Gold.
(
    sessoes
    .write
    .mode("overwrite")
    .parquet(caminho("gold/sessoes"))
)

# Apresenta os resultados.
titulo("Sessões - Gold")

print(f"Total de sessões: {sessoes.count()}")

sessoes.orderBy("session_id").show(10, truncate=False)

print("Tabela gold/sessoes gravada com sucesso.")

spark.stop()
