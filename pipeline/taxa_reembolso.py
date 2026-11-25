
from pyspark.sql import functions as F

from comum import criar_sessao, ler_eventos, caminho, titulo


spark = criar_sessao("taxa_reembolso")

# 1. Ler eventos válidos da Silver.
eventos = ler_eventos(spark, "silver")

# 2. Consolidar compras por transação.
compras_por_transacao = (
    eventos
    .filter(F.col("event_name") == "purchase")
    .groupBy("transaction_id")
    .agg(
        F.first(
            "session_id",
            ignorenulls=True
        ).alias("session_id")
    )
)

# 3. Identificar transações que tiveram reembolso.
reembolsos = (
    eventos
    .filter(F.col("event_name") == "refund")
    .select("transaction_id")
    .distinct()
    .withColumn("teve_reembolso", F.lit(1))
)

# 4. Identificar o plano de cada compra.
itens = ler_eventos(spark, "silver/itens")

planos = (
    itens
    .filter(
        (F.col("event_name") == "purchase") &
        (F.col("item_category") == "plano")
    )
    .groupBy("transaction_id")
    .agg(
        F.first(
            "item_id",
            ignorenulls=True
        ).alias("plano")
    )
)

# 5. Recuperar o canal de aquisição da sessão.
sessoes = spark.read.parquet(caminho("gold/sessoes"))

canais = sessoes.select(
    "session_id",
    "canal"
)

# 6. Relacionar compras, planos, reembolsos e canais.
compras_com_contexto = (
    compras_por_transacao
    .join(planos, on="transaction_id", how="left")
    .join(canais, on="session_id", how="left")
    .join(reembolsos, on="transaction_id", how="left")
    .withColumn(
        "plano",
        F.coalesce(F.col("plano"), F.lit("(nenhum)"))
    )
    .withColumn(
        "canal",
        F.coalesce(F.col("canal"), F.lit("(desconhecido)"))
    )
    .withColumn(
        "teve_reembolso",
        F.coalesce(F.col("teve_reembolso"), F.lit(0))
    )
)

# 7. Calcular pedidos e taxa de reembolso por plano e canal.
taxa = (
    compras_com_contexto
    .groupBy("plano", "canal")
    .agg(
        F.countDistinct("transaction_id").alias("pedidos"),
        F.sum("teve_reembolso").alias("pedidos_reembolsados")
    )
    .withColumn(
        "taxa_reembolso",
        F.round(
            F.col("pedidos_reembolsados") / F.col("pedidos"),
            4
        )
    )
    .orderBy("plano", "canal")
)

# 8. Gravar tabela Gold.
destino = caminho("gold/taxa_reembolso")

taxa.write.mode("overwrite").parquet(destino)

print("\nTAXA DE REEMBOLSO POR PLANO E CANAL:")
taxa.show(30, truncate=False)

print("\nTOTAL DE LINHAS:", taxa.count())

titulo("Tabela gold/taxa_reembolso gravada com sucesso.")

spark.stop()
