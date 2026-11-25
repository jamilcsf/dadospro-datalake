
from pyspark.sql import functions as F

from comum import criar_sessao, ler_eventos, caminho, titulo


spark = criar_sessao("adesao_order_bump")

# 1. Ler os itens dos eventos Silver.
itens = ler_eventos(spark, "silver/itens")

# 2. Identificar compras com pelo menos um Order Bump.
compras = (
    itens
    .filter(F.col("event_name") == "purchase")
    .groupBy("transaction_id")
    .agg(
        F.max(
            F.when(
                F.col("item_category") == "order_bump",
                1
            ).otherwise(0)
        ).alias("comprou_order_bump")
    )
)

# 3. Calcular o total de compras e a adesão.
resultado = (
    compras
    .agg(
        F.count("*").alias("total_compras"),
        F.sum("comprou_order_bump").alias("compras_com_order_bump")
    )
    .withColumn(
        "adesao_percentual",
        F.round(
            F.col("compras_com_order_bump") /
            F.col("total_compras"),
            4
        )
    )
)

# 4. Gravar tabela Gold.
resultado.write.mode("overwrite").parquet(
    caminho("gold/adesao_order_bump")
)

print("\nADESÃO AO ORDER BUMP:")
resultado.show(truncate=False)

titulo("Tabela gold/adesao_order_bump gravada com sucesso.")

spark.stop()
