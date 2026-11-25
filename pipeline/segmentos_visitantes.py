"""Gold — segmentação de visitantes por comportamento.

Executar na raiz do projeto:
    docker compose run --rm app python pipeline/segmentos_visitantes.py

A lógica de perfil e segmentação segue a implementação de referência do professor
em professor/solucao/06_gold_ml.py.
"""

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from comum import (
    caminho,
    criar_sessao,
    exigir_camada,
    glob_eventos,
    ler_eventos,
    titulo,
)


SEED = 42
PERFIL = [
    "sessoes",
    "eventos_medios",
    "scroll_medio",
    "taxa_video",
    "fez_lead",
    "comprou",
]


def perfil_visitantes(eventos: DataFrame) -> DataFrame:
    """Resume o comportamento das sessões em um perfil por visitante."""
    e = F.col("event_name")

    por_sessao = eventos.groupBy("user_pseudo_id", "session_id").agg(
        F.count("*").alias("eventos"),
        F.coalesce(F.max("percent_scrolled"), F.lit(0)).alias("scroll_max"),
        F.max((e == "video_start").cast("int")).alias("viu_video"),
        F.max((e == "generate_lead").cast("int")).alias("fez_lead"),
        F.max((e == "purchase").cast("int")).alias("comprou"),
    )

    return por_sessao.groupBy(
        F.col("user_pseudo_id").alias("visitante")
    ).agg(
        F.count("*").cast("double").alias("sessoes"),
        F.avg("eventos").alias("eventos_medios"),
        F.avg("scroll_max").alias("scroll_medio"),
        F.avg("viu_video").alias("taxa_video"),
        F.max("fez_lead").cast("double").alias("fez_lead"),
        F.max("comprou").cast("double").alias("comprou"),
    )


def segmentar(perfis: DataFrame, ks=(3, 4, 5)) -> DataFrame:
    """Testa diferentes quantidades de grupos e escolhe a maior silhueta."""
    # A ordenação e a partição única seguem a referência do professor
    # para tornar o resultado reprodutível diante de diferentes layouts S3.
    perfis = perfis.orderBy("visitante").coalesce(1)

    vetor = VectorAssembler(
        inputCols=PERFIL,
        outputCol="bruto",
    ).transform(perfis)

    escalado = (
        StandardScaler(inputCol="bruto", outputCol="features")
        .fit(vetor)
        .transform(vetor)
        .cache()
    )

    avaliador = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="segmento",
    )

    melhor = None

    for k in ks:
        modelo = KMeans(
            k=k,
            seed=SEED,
            featuresCol="features",
            predictionCol="segmento",
        ).fit(escalado)

        silhueta = avaliador.evaluate(modelo.transform(escalado))

        if melhor is None or silhueta > melhor[1]:
            melhor = (k, silhueta, modelo)

    k, silhueta, modelo = melhor

    segmentado = modelo.transform(escalado).select(
        "visitante",
        "segmento",
        *PERFIL,
    )

    perfil = segmentado.groupBy("segmento").agg(
        F.count("*").alias("visitantes_no_segmento"),
        *[
            F.round(F.avg(coluna), 3).alias(f"media_{coluna}")
            for coluna in PERFIL
        ],
    )

    return (
        segmentado.select("visitante", "segmento")
        .join(perfil, "segmento")
        .withColumn("k", F.lit(k))
        .withColumn("silhueta", F.lit(round(silhueta, 4)))
    )


def main() -> None:
    spark = criar_sessao("gold_segmentos_visitantes")

    exigir_camada(
        spark,
        glob_eventos("silver"),
        "python pipeline/silver.py",
    )

    eventos = ler_eventos(spark, "silver").cache()

    perfis = perfil_visitantes(eventos)
    segmentos = segmentar(perfis).cache()

    segmentos.coalesce(1).write.mode("overwrite").parquet(
        caminho("gold/segmentos_visitantes")
    )

    titulo("Segmentação de visitantes")

    primeira_linha = segmentos.first()
    print(f"Quantidade de segmentos escolhida (k): {primeira_linha.k}")
    print(f"Silhueta: {primeira_linha.silhueta}")

    print("\nResumo dos segmentos:")
    (
        segmentos.drop("visitante")
        .distinct()
        .orderBy("segmento")
        .show(truncate=False)
    )

    print("\nQuantidade de visitantes por segmento:")
    (
        segmentos.groupBy("segmento")
        .agg(F.count("*").alias("visitantes"))
        .orderBy("segmento")
        .show(truncate=False)
    )

    titulo("Tabela gold/segmentos_visitantes gravada com sucesso")

    segmentos.unpersist()
    eventos.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
