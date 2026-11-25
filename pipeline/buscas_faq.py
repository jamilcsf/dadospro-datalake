
from pyspark.sql import functions as F

from comum import (
    criar_sessao,
    caminho,
    ler_eventos,
    titulo,
)

# Inicia a sessão Spark.
spark = criar_sessao("gold_buscas_faq")

# Lê os eventos da Silver.
silver = ler_eventos(spark, "silver")

# Seleciona as pesquisas e extrai as informações.
buscas = (
    silver
    .filter(F.col("event_name") == "site_search")
    .select(
        F.trim(F.col("params.search_term")).alias("termo"),
        F.col("params.search_results").cast("long").alias("resultados"),
    )
    .filter(
        F.col("termo").isNotNull()
        & (F.col("termo") != "")
    )
)

# Agrupa por termo pesquisado.
buscas_faq = (
    buscas
    .groupBy("termo")
    .agg(
        F.count("*").alias("total_buscas"),
        F.sum(
            F.when(F.col("resultados") == 0, 1).otherwise(0)
        ).alias("buscas_sem_resultado"),
    )
    .orderBy(F.desc("total_buscas"), F.asc("termo"))
)

# Grava a tabela Gold.
buscas_faq.write.mode("overwrite").parquet(
    caminho("gold/buscas_faq")
)

titulo("Buscas FAQ - Gold")

print("Total de pesquisas:", buscas.count())
buscas_faq.show(truncate=False)

print("Tabela gold/buscas_faq gravada com sucesso.")

spark.stop()
