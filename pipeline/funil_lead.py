
from pyspark.sql import functions as F

from comum import criar_sessao, caminho, ler_eventos, titulo


# Inicia a sessão Spark.
spark = criar_sessao("gold_funil_lead")

# Lê os eventos válidos da Silver.
silver = ler_eventos(spark, "silver")

# Considera somente eventos da landing page.
landing = silver.filter(
    F.col("page.location") == "https://loja.exemplo/"
)

# Identifica o canal do primeiro evento de cada sessão na landing.
primeiro_evento = (
    landing
    .groupBy("session_id")
    .agg(
        F.min_by(
            F.struct(
                F.col("event_ts"),
                F.col("event_id"),
                F.col("traffic.source"),
                F.col("traffic.medium"),
            ),
            F.struct(F.col("event_ts"), F.col("event_id")),
        ).alias("primeiro")
    )
    .select(
        "session_id",
        F.concat_ws(
            " / ",
            F.coalesce(F.col("primeiro.source"), F.lit("(direct)")),
            F.coalesce(F.col("primeiro.medium"), F.lit("(none)")),
        ).alias("canal"),
    )
)

# Conta se cada sessão atingiu as etapas do funil.
sessoes = (
    landing
    .groupBy("session_id")
    .agg(
        F.max(
            F.when(F.col("event_name") == "page_view", 1).otherwise(0)
        ).alias("visualizou_landing"),
        F.max(
            F.when(
                (F.col("event_name") == "scroll")
                & (F.col("params.percent_scrolled") >= 50),
                1,
            ).otherwise(0)
        ).alias("rolou_50_porcento"),
        F.max(
            F.when(F.col("event_name") == "generate_lead", 1).otherwise(0)
        ).alias("gerou_lead"),
    )
    .join(primeiro_evento, on="session_id", how="left")
)

# Consolida os resultados por canal.
por_canal = (
    sessoes
    .groupBy("canal")
    .agg(
        F.sum("visualizou_landing").alias("visualizou_landing"),
        F.sum("rolou_50_porcento").alias("rolou_50_porcento"),
        F.sum("gerou_lead").alias("gerou_lead"),
    )
)

# Calcula também o total geral.
total_geral = (
    sessoes
    .agg(
        F.sum("visualizou_landing").alias("visualizou_landing"),
        F.sum("rolou_50_porcento").alias("rolou_50_porcento"),
        F.sum("gerou_lead").alias("gerou_lead"),
    )
    .withColumn("canal", F.lit("(todos)"))
    .select("canal", "visualizou_landing", "rolou_50_porcento", "gerou_lead")
)

resumo = por_canal.unionByName(total_geral)

# Transforma as etapas em linhas.
funil = (
    resumo
    .select(
        "canal",
        F.explode(
            F.array(
                F.struct(
                    F.lit("page_view").alias("etapa"),
                    F.col("visualizou_landing").alias("sessoes"),
                    F.lit(1).alias("ordem"),
                ),
                F.struct(
                    F.lit("scroll_50").alias("etapa"),
                    F.col("rolou_50_porcento").alias("sessoes"),
                    F.lit(2).alias("ordem"),
                ),
                F.struct(
                    F.lit("generate_lead").alias("etapa"),
                    F.col("gerou_lead").alias("sessoes"),
                    F.lit(3).alias("ordem"),
                ),
            )
        ).alias("item"),
    )
    .select(
        "canal",
        F.col("item.etapa").alias("etapa"),
        F.col("item.sessoes").alias("sessoes"),
        F.col("item.ordem").alias("ordem"),
    )
)

# Calcula as taxas de conversão.
from pyspark.sql.window import Window

janela = Window.partitionBy("canal").orderBy("ordem")
janela_completa = janela.rowsBetween(
    Window.unboundedPreceding,
    Window.unboundedFollowing,
)

funil = (
    funil
    .withColumn("etapa_anterior", F.lag("sessoes").over(janela))
    .withColumn(
        "total_primeira_etapa",
        F.first("sessoes").over(janela_completa),
    )
    .withColumn(
        "conversao_etapa_anterior",
        F.when(
            F.col("etapa_anterior") > 0,
            F.round(F.col("sessoes") / F.col("etapa_anterior"), 4),
        ),
    )
    .withColumn(
        "conversao_topo",
        F.when(
            F.col("total_primeira_etapa") > 0,
            F.round(F.col("sessoes") / F.col("total_primeira_etapa"), 4),
        ),
    )
    .select(
        "canal",
        "etapa",
        "ordem",
        "sessoes",
        "conversao_etapa_anterior",
        "conversao_topo",
    )
)

# Grava a tabela Gold.
(
    funil
    .write
    .mode("overwrite")
    .parquet(caminho("gold/funil_lead"))
)

# Apresenta o resultado geral.
titulo("Funil de leads - Gold")

funil.filter(F.col("canal") == "(todos)").orderBy("ordem").show(
    truncate=False
)

print("Tabela gold/funil_lead gravada com sucesso.")

spark.stop()
