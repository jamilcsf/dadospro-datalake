
from pyspark.sql import functions as F

from comum import (
    criar_sessao,
    ler_eventos,
    caminho,
    titulo,
)

spark = criar_sessao("receita_diaria")

# 1. Ler eventos válidos da Silver
eventos = ler_eventos(spark, "silver")

# 2. Separar compras e reembolsos
compras_eventos = eventos.filter(
    F.col("event_name") == "purchase"
)

reembolsos_eventos = eventos.filter(
    F.col("event_name") == "refund"
)

# 3. Consolidar compras por transação
compras = (
    compras_eventos
    .groupBy("transaction_id")
    .agg(
        F.first("event_ts", ignorenulls=True).alias("event_ts"),
        F.first("session_id", ignorenulls=True).alias("session_id"),
        F.first(
            F.col("params.value").cast("double"),
            ignorenulls=True
        ).alias("receita_bruta")
    )
    .withColumn("data", F.to_date("event_ts"))
)

# 4. Identificar o plano de cada compra
itens = ler_eventos(spark, "silver/itens")

planos = (
    itens
    .filter(
        (F.col("event_name") == "purchase") &
        (F.col("item_category") == "plano")
    )
    .groupBy("transaction_id")
    .agg(
        F.first("item_id", ignorenulls=True).alias("plano")
    )
)

# 5. Somar reembolsos por transação
reembolsos = (
    reembolsos_eventos
    .groupBy("transaction_id")
    .agg(
        F.sum(
            F.col("params.value").cast("double")
        ).alias("receita_reembolsada")
    )
)

# 6. Recuperar o canal de aquisição da sessão
sessoes = spark.read.parquet(caminho("gold/sessoes"))

canais = sessoes.select(
    "session_id",
    "canal"
)

# 7. Relacionar compras, planos, reembolsos e canais
receitas = (
    compras
    .join(planos, on="transaction_id", how="left")
    .join(reembolsos, on="transaction_id", how="left")
    .join(canais, on="session_id", how="left")
    .withColumn(
        "plano",
        F.coalesce(F.col("plano"), F.lit("(nenhum)"))
    )
    .withColumn(
        "canal",
        F.coalesce(F.col("canal"), F.lit("(desconhecido)"))
    )
    .withColumn(
        "receita_reembolsada",
        F.coalesce(F.col("receita_reembolsada"), F.lit(0.0))
    )
    .withColumn(
        "receita_liquida",
        F.col("receita_bruta") - F.col("receita_reembolsada")
    )
)

# 8. Agregar por data, plano e canal
receita_diaria = (
    receitas
    .groupBy("data", "plano", "canal")
    .agg(
        F.round(F.sum("receita_bruta"), 2).alias("receita_bruta"),
        F.round(F.sum("receita_reembolsada"), 2).alias("receita_reembolsada"),
        F.round(F.sum("receita_liquida"), 2).alias("receita_liquida"),
        F.countDistinct("transaction_id").alias("pedidos"),
        F.round(
            F.sum("receita_bruta") /
            F.countDistinct("transaction_id"),
            2
        ).alias("ticket_medio")
    )
    .orderBy("data", "plano", "canal")
)

# 9. Gravar tabela Gold
destino = caminho("gold/receita_diaria")

receita_diaria.write.mode("overwrite").parquet(destino)

print("\nAMOSTRA DA RECEITA DIÁRIA:")
receita_diaria.show(30, truncate=False)

print("\nTOTAL DE LINHAS:", receita_diaria.count())

titulo("Tabela gold/receita_diaria gravada com sucesso.")

spark.stop()
