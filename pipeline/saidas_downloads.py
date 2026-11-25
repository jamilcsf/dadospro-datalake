
from pyspark.sql import functions as F

from comum import (
    criar_sessao,
    caminho,
    ler_eventos,
    titulo,
)

spark = criar_sessao("gold_saidas_downloads")

silver = ler_eventos(spark, "silver")

# Cliques externos agrupados por domínio.
cliques = (
    silver
    .filter(
        (F.col("event_name") == "click")
        & (F.col("params.outbound") == True)
    )
    .filter(F.col("params.link_domain").isNotNull())
    .groupBy(F.col("params.link_domain").alias("destino"))
    .agg(F.count("*").alias("total_eventos"))
    .withColumn("tipo", F.lit("clique_externo"))
)

# Downloads agrupados por nome do arquivo.
downloads = (
    silver
    .filter(F.col("event_name") == "file_download")
    .filter(F.col("params.file_name").isNotNull())
    .groupBy(F.col("params.file_name").alias("destino"))
    .agg(F.count("*").alias("total_eventos"))
    .withColumn("tipo", F.lit("download"))
)

# Une os dois resultados em uma tabela.
saidas_downloads = (
    cliques
    .unionByName(downloads)
    .select("tipo", "destino", "total_eventos")
    .orderBy("tipo", F.desc("total_eventos"))
)

# Grava a tabela Gold.
saidas_downloads.write.mode("overwrite").parquet(
    caminho("gold/saidas_downloads")
)

titulo("Saídas e downloads - Gold")

saidas_downloads.show(truncate=False)

print("Tabela gold/saidas_downloads gravada com sucesso.")

spark.stop()
