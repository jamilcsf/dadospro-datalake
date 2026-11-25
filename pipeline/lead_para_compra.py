from pyspark.sql import Window
from pyspark.sql import functions as F

from comum import (
    caminho,
    criar_sessao,
    exigir_camada,
    glob_eventos,
    ler_eventos,
    titulo,
)


TODOS = "(todos)"

spark = criar_sessao("lead_para_compra")

# Verifica se as camadas necessárias existem antes de iniciar o cálculo.
exigir_camada(
    spark,
    glob_eventos("silver"),
    "python pipeline/silver.py",
)

exigir_camada(
    spark,
    "gold/sessoes",
    "python pipeline/sessoes.py",
)

# Lê os eventos válidos da Silver e os dados de sessão da Gold.
eventos = ler_eventos(spark, "silver")
sessoes = spark.read.parquet(caminho("gold/sessoes"))

# Identifica a primeira geração de lead de cada pessoa.
# visitante_id já contém a identidade resolvida na Silver.
janela_lead = Window.partitionBy("visitante_id").orderBy(
    "event_ts",
    "event_id",
)

leads = (
    eventos
    .filter(
        (F.col("event_name") == "generate_lead") &
        F.col("visitante_id").isNotNull()
    )
    .withColumn(
        "_ordem",
        F.row_number().over(janela_lead)
    )
    .filter(F.col("_ordem") == 1)
    # Recupera o canal da sessão em que ocorreu o primeiro lead.
    .join(
        sessoes.select("session_id", "canal"),
        on="session_id",
        how="left",
    )
    .select(
        F.col("visitante_id").alias("pessoa_id"),
        F.col("event_ts").alias("data_lead"),
        "canal",
    )
    .fillna({"canal": "(desconhecido)"})
)

# Seleciona as compras de cada pessoa.
compras = (
    eventos
    .filter(
        (F.col("event_name") == "purchase") &
        F.col("visitante_id").isNotNull()
    )
    .select(
        F.col("visitante_id").alias("pessoa_id"),
        F.col("event_ts").alias("data_compra"),
    )
)

# Considera somente compras estritamente posteriores ao primeiro lead.
# Se houver mais de uma, mantém a primeira.
compras_posteriores = (
    compras
    .join(
        leads.select("pessoa_id", "data_lead"),
        on="pessoa_id",
        how="inner",
    )
    .filter(F.col("data_compra") > F.col("data_lead"))
    .groupBy("pessoa_id")
    .agg(
        F.min("data_compra").alias("data_compra")
    )
)

# Mantém todos os leads, inclusive aqueles que não viraram compras.
base = (
    leads
    .join(
        compras_posteriores,
        on="pessoa_id",
        how="left",
    )
    .withColumn(
        "dias",
        (
            F.col("data_compra").cast("long") -
            F.col("data_lead").cast("long")
        ) / 86400.0
    )
)

# Calcula os indicadores por canal.
por_canal = (
    base
    .groupBy("canal")
    .agg(
        F.count("*").alias("leads"),
        F.count("data_compra").alias("compraram_depois"),
        F.round(
            F.percentile_approx("dias", 0.5),
            2,
        ).alias("mediana_dias"),
    )
)

# Calcula também o resultado geral, identificado como "(todos)".
total = (
    base
    .withColumn("canal", F.lit(TODOS))
    .groupBy("canal")
    .agg(
        F.count("*").alias("leads"),
        F.count("data_compra").alias("compraram_depois"),
        F.round(
            F.percentile_approx("dias", 0.5),
            2,
        ).alias("mediana_dias"),
    )
)

# Une os resultados e calcula a taxa de conversão.
resultado = (
    por_canal
    .unionByName(total)
    .withColumn(
        "taxa_conversao",
        F.round(
            F.try_divide(
                F.col("compraram_depois"),
                F.col("leads"),
            ),
            4,
        ),
    )
    .select(
        "canal",
        "leads",
        "compraram_depois",
        "mediana_dias",
        "taxa_conversao",
    )
)

# Grava a tabela Gold.
resultado.write.mode("overwrite").parquet(
    caminho("gold/lead_para_compra")
)

print("\nCONVERSÃO DE LEADS EM COMPRAS:")
resultado.orderBy("canal").show(truncate=False)

titulo("Tabela gold/lead_para_compra gravada com sucesso.")

spark.stop()
