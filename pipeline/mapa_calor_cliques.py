"""Gold — mapa de calor de cliques por elemento e página.

Executar na raiz do projeto:
    docker compose run --rm app python pipeline/mapa_calor_cliques.py
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo


def pagina_do_evento(coluna: str = "page_location"):
    """Converte URL completa em caminho, removendo domínio, query e fragmento."""
    caminho_url = F.regexp_extract(F.col(coluna), r"^[a-z]+://[^/?#]+([^?#]*)", 1)
    return (
        F.when(F.col(coluna).isNull(), F.lit(None))
        .when(caminho_url == "", F.lit("/"))
        .otherwise(caminho_url)
    )


def mapa_cliques(eventos):
    """Calcula cliques, sessões, taxa e intensidade por elemento e página."""
    com_pagina = (
        eventos.withColumn("pagina", pagina_do_evento())
        .filter(F.col("pagina").isNotNull())
    )

    cliques = (
        com_pagina.filter(F.col("element_id").isNotNull())
        .groupBy("pagina", "element_id")
        .agg(
            F.count("*").alias("cliques"),
            F.countDistinct("session_id").alias("sessoes"),
        )
    )

    visitas = (
        com_pagina.filter(F.col("event_name") == "page_view")
        .select("pagina", "session_id")
        .distinct()
        .groupBy("pagina")
        .agg(F.count("*").alias("sessoes_na_pagina"))
    )

    maximo = F.max("cliques").over(Window.partitionBy("pagina"))
    base = F.col("sessoes_na_pagina")

    return (
        cliques.join(visitas, "pagina", "left")
        .withColumn(
            "sessoes_na_pagina",
            F.coalesce(base, F.lit(0)).cast("long"),
        )
        .withColumn(
            "taxa_clique",
            F.when(base > 0, F.round(F.col("sessoes") / base, 4)),
        )
        .withColumn("intensidade", F.round(F.col("cliques") / maximo, 4))
        .select(
            "pagina",
            "element_id",
            "cliques",
            "sessoes",
            "sessoes_na_pagina",
            "taxa_clique",
            "intensidade",
        )
        .orderBy("pagina", F.desc("cliques"), "element_id")
    )


def main() -> None:
    spark = criar_sessao("gold-mapa-calor-cliques")

    exigir_camada(
        spark,
        glob_eventos("silver"),
        "python pipeline/silver.py",
    )

    eventos = ler_eventos(spark, "silver").cache()
    resultado = mapa_cliques(eventos)

    resultado.coalesce(1).write.mode("overwrite").parquet(
        caminho("gold/mapa_calor_cliques")
    )

    titulo("Gold — mapa de calor de cliques")
    spark.read.parquet(caminho("gold/mapa_calor_cliques")).show(
        40, truncate=False
    )

    eventos.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
