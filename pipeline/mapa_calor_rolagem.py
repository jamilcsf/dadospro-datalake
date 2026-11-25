"""Gold — mapa de calor de rolagem da landing page.

Executar na raiz do projeto:
    docker compose run --rm app python pipeline/mapa_calor_rolagem.py
"""

from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo


MARCOS = [0, 25, 50, 75, 90]
LANDING = "/"


def pagina_do_evento(coluna: str = "page_location"):
    """Converte URL completa em caminho, removendo domínio, query e fragmento."""
    caminho_url = F.regexp_extract(F.col(coluna), r"^[a-z]+://[^/?#]+([^?#]*)", 1)
    return (
        F.when(F.col(coluna).isNull(), F.lit(None))
        .when(caminho_url == "", F.lit("/"))
        .otherwise(caminho_url)
    )


def mapa_rolagem(eventos):
    """Conta sessões da landing que chegaram a cada marco de rolagem."""
    com_pagina = (
        eventos.withColumn("pagina", pagina_do_evento())
        .filter(F.col("pagina").isNotNull())
    )

    base = (
        com_pagina.filter(F.col("event_name") == "page_view")
        .filter(F.col("pagina") == LANDING)
        .select("session_id")
        .distinct()
    )

    maximo = (
        com_pagina.filter(
            (F.col("pagina") == LANDING)
            & (F.col("event_name") == "scroll")
        )
        .groupBy("session_id")
        .agg(F.max("percent_scrolled").alias("maximo"))
    )

    por_sessao = base.join(maximo, "session_id", "left").fillna(
        0, ["maximo"]
    )

    somas = por_sessao.agg(
        *[
            F.sum((F.col("maximo") >= marco).cast("int")).alias(f"m{marco}")
            for marco in MARCOS
        ]
    )

    pilha = ", ".join(f"{marco}, m{marco}" for marco in MARCOS)

    return (
        somas.select(
            F.col("m0").alias("total"),
            F.expr(
                f"stack({len(MARCOS)}, {pilha}) "
                "as (marco, sessoes_que_chegaram)"
            ),
        )
        .select(
            F.lit(LANDING).alias("pagina"),
            F.col("marco").cast("int").alias("marco"),
            F.coalesce(
                F.col("sessoes_que_chegaram"), F.lit(0)
            ).cast("long").alias("sessoes_que_chegaram"),
            F.when(
                F.col("total") > 0,
                F.round(F.col("sessoes_que_chegaram") / F.col("total"), 4),
            ).alias("pct_sessoes"),
        )
        .orderBy("marco")
    )


def main() -> None:
    spark = criar_sessao("gold-mapa-calor-rolagem")

    exigir_camada(
        spark,
        glob_eventos("silver"),
        "python pipeline/silver.py",
    )

    eventos = ler_eventos(spark, "silver").cache()
    resultado = mapa_rolagem(eventos)

    resultado.coalesce(1).write.mode("overwrite").parquet(
        caminho("gold/mapa_calor_rolagem")
    )

    titulo("Gold — mapa de calor de rolagem")
    spark.read.parquet(caminho("gold/mapa_calor_rolagem")).show(
        40, truncate=False
    )

    eventos.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
