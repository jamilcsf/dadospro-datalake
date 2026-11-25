
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from comum import criar_sessao, caminho, ler_eventos, titulo


# Inicia a sessão Spark.
spark = criar_sessao("gold_funil_compra")

# Lê os eventos da Silver.
silver = ler_eventos(spark, "silver")

# Lê os itens individualizados da Silver.
itens = spark.read.parquet(caminho("silver/itens"))

# Define as etapas do funil de compra.
etapas = [
    ("view_item_list", "visualizou_lista"),
    ("select_item", "selecionou_item"),
    ("add_to_cart", "adicionou_carrinho"),
    ("view_cart", "visualizou_carrinho"),
    ("begin_checkout", "iniciou_checkout"),
    ("add_payment_info", "informou_pagamento"),
    ("purchase", "comprou"),
]

# Identifica a primeira origem e mídia de cada sessão.
janela_primeiro_evento = (
    Window
    .partitionBy("session_id")
    .orderBy("event_ts", "event_id")
)

primeiro_evento = (
    silver
    .withColumn(
        "_ordem",
        F.row_number().over(janela_primeiro_evento),
    )
    .filter(F.col("_ordem") == 1)
    .select(
        "session_id",
        F.concat_ws(
            " / ",
            F.coalesce(F.col("traffic.source"), F.lit("(direct)")),
            F.coalesce(F.col("traffic.medium"), F.lit("(none)")),
        ).alias("canal"),
    )
)

# Identifica o primeiro plano selecionado em cada sessão.
janela_primeiro_item = (
    Window
    .partitionBy("session_id")
    .orderBy("event_ts", "event_id", "item_index")
)

primeiro_plano = (
    itens
    .filter(F.col("event_name") == "select_item")
    .withColumn(
        "_ordem",
        F.row_number().over(janela_primeiro_item),
    )
    .filter(F.col("_ordem") == 1)
    .select(
        "session_id",
        F.col("item_id").alias("plano"),
    )
)

# Cria indicadores de presença de cada etapa por sessão.
indicadores = [
    F.max(
        F.when(F.col("event_name") == evento, 1).otherwise(0)
    ).alias(coluna)
    for evento, coluna in etapas
]

sessoes = (
    silver
    .groupBy("session_id")
    .agg(*indicadores)
    .join(primeiro_evento, on="session_id", how="left")
    .join(primeiro_plano, on="session_id", how="left")
    .withColumn(
        "plano",
        F.coalesce(F.col("plano"), F.lit("(nenhum)")),
    )
)

# Calcula os totais por plano e canal.
colunas_indicadores = [coluna for _, coluna in etapas]

por_dimensao = (
    sessoes
    .groupBy("plano", "canal")
    .agg(
        *[
            F.sum(F.col(coluna)).alias(coluna)
            for coluna in colunas_indicadores
        ]
    )
)

# Calcula também o total geral.
total_geral = sessoes.agg(
    *[
        F.sum(F.col(coluna)).alias(coluna)
        for coluna in colunas_indicadores
    ]
).withColumn("plano", F.lit("(todos)")).withColumn(
    "canal", F.lit("(todos)")
).select("plano", "canal", *colunas_indicadores)

resumo = por_dimensao.unionByName(total_geral)

# Transforma as etapas em linhas.
linhas_etapas = [
    F.struct(
        F.lit(evento).alias("etapa"),
        F.col(coluna).alias("sessoes"),
        F.lit(ordem).alias("ordem"),
    )
    for ordem, (evento, coluna) in enumerate(etapas, start=1)
]

funil = (
    resumo
    .select(
        "plano",
        "canal",
        F.explode(F.array(*linhas_etapas)).alias("item"),
    )
    .select(
        "plano",
        "canal",
        F.col("item.etapa").alias("etapa"),
        F.col("item.sessoes").alias("sessoes"),
        F.col("item.ordem").alias("ordem"),
    )
)

# Calcula as taxas de conversão.
janela_funil = (
    Window
    .partitionBy("plano", "canal")
    .orderBy("ordem")
)

janela_completa = janela_funil.rowsBetween(
    Window.unboundedPreceding,
    Window.unboundedFollowing,
)

funil = (
    funil
    .withColumn(
        "etapa_anterior",
        F.lag("sessoes").over(janela_funil),
    )
    .withColumn(
        "total_primeira_etapa",
        F.first("sessoes").over(janela_completa),
    )
    .withColumn(
        "conversao_etapa_anterior",
        F.when(
            F.col("etapa_anterior").isNotNull()
            & (F.col("etapa_anterior") > 0),
            F.round(
                F.col("sessoes") / F.col("etapa_anterior"),
                4,
            ),
        ),
    )
    .withColumn(
        "conversao_topo",
        F.when(
            F.col("total_primeira_etapa") > 0,
            F.round(
                F.col("sessoes") / F.col("total_primeira_etapa"),
                4,
            ),
        ),
    )
    .select(
        "plano",
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
    .parquet(caminho("gold/funil_compra"))
)

# Apresenta os resultados.
titulo("Funil de compra - Gold")

funil.filter(
    (F.col("plano") == "(todos)")
    & (F.col("canal") == "(todos)")
).orderBy("ordem").show(truncate=False)

print("Tabela gold/funil_compra gravada com sucesso.")

spark.stop()
