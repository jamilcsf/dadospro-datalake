"""Gold — propensão por sessão, métricas do modelo e segmentos de visitantes.

Executar na raiz do projeto:
    docker compose run --rm -e AWS_S3_BUCKET=datalake-tracking-teste app python pipeline/06_gold_ml.py

As três tabelas são gravadas independentemente:
    gold/propensao_sessao
    gold/metricas_modelo
    gold/segmentos_visitantes
"""

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo


SEED = 42
FEATURES = [
    "visualizacoes_pagina",
    "scroll_maximo_pre_carrinho",
    "video_maximo_pre_carrinho",
    "inicios_video",
    "buscas",
    "buscas_sem_resultado",
    "cliques",
    "cliques_externos",
    "downloads",
    "leads",
    "itens_selecionados",
    "visualizacoes_produto",
    "visualizacoes_lista",
]
PERFIL = [
    "sessoes",
    "eventos_medios",
    "scroll_medio",
    "taxa_video",
    "fez_lead",
    "comprou",
]


def _canal(source, medium):
    return F.concat_ws(
        " / ",
        F.coalesce(source, F.lit("(direct)")),
        F.coalesce(medium, F.lit("(none)")),
    )


def construir_base_sessoes(eventos: DataFrame) -> DataFrame:
    """Uma linha por sessão, com rótulo e atributos do primeiro evento."""
    ordem = F.struct(F.col("event_ts"), F.col("event_id"))
    return (
        eventos.groupBy("session_id")
        .agg(
            F.first("user_pseudo_id", ignorenulls=True).alias("_pseudo"),
            F.first("visitante_id", ignorenulls=True).alias("visitante_id"),
            F.max((F.col("event_name") == "purchase").cast("int")).alias("label"),
            F.min_by(
                F.struct(
                    F.col("traffic.source").alias("source"),
                    F.col("traffic.medium").alias("medium"),
                    F.col("device.category").alias("dispositivo"),
                ),
                ordem,
            ).alias("_primeiro"),
        )
        .withColumn(
            "chave_visitante",
            F.coalesce("_pseudo", "visitante_id", "session_id"),
        )
        .withColumn("canal", _canal(F.col("_primeiro.source"), F.col("_primeiro.medium")))
        .withColumn("dispositivo", F.col("_primeiro.dispositivo"))
        .select("session_id", "visitante_id", "chave_visitante", "canal", "dispositivo",
                F.col("label").cast("double").alias("label"))
    )


def construir_features(eventos: DataFrame) -> DataFrame:
    """Features comportamentais da landing estritamente anteriores ao primeiro carrinho."""
    primeiro_carrinho = (
        eventos.filter(F.col("event_name") == "add_to_cart")
        .groupBy("session_id")
        .agg(F.min("event_ts").alias("_primeiro_carrinho"))
    )

    landing = eventos.filter(
        F.col("page_location") == "https://loja.exemplo/"
    )
    comportamento = (
        landing.join(primeiro_carrinho, "session_id", "left")
        .filter(
            F.col("_primeiro_carrinho").isNull()
            | (F.col("event_ts") < F.col("_primeiro_carrinho"))
        )
    )

    return comportamento.groupBy("session_id").agg(
        F.sum((F.col("event_name") == "page_view").cast("int")).alias("visualizacoes_pagina"),
        F.max(F.when(F.col("event_name") == "scroll", F.col("percent_scrolled"))).alias("scroll_maximo_pre_carrinho"),
        F.max(F.when(F.col("event_name").isin("video_start", "video_progress", "video_complete"),
                     F.col("video_percent"))).alias("video_maximo_pre_carrinho"),
        F.sum((F.col("event_name") == "video_start").cast("int")).alias("inicios_video"),
        F.sum((F.col("event_name") == "site_search").cast("int")).alias("buscas"),
        F.sum(((F.col("event_name") == "site_search") & (F.col("search_results") == 0)).cast("int")).alias("buscas_sem_resultado"),
        F.sum((F.col("event_name") == "click").cast("int")).alias("cliques"),
        F.sum(((F.col("event_name") == "click") & (F.col("outbound") == True)).cast("int")).alias("cliques_externos"),
        F.sum((F.col("event_name") == "file_download").cast("int")).alias("downloads"),
        F.sum((F.col("event_name") == "generate_lead").cast("int")).alias("leads"),
        F.sum((F.col("event_name") == "select_item").cast("int")).alias("itens_selecionados"),
        F.sum((F.col("event_name") == "view_item").cast("int")).alias("visualizacoes_produto"),
        F.sum((F.col("event_name") == "view_item_list").cast("int")).alias("visualizacoes_lista"),
    )


def dividir_por_visitante(base: DataFrame):
    """Split determinístico por visitante: um visitante nunca aparece em mais de um conjunto."""
    bucket = F.pmod(F.xxhash64("chave_visitante"), F.lit(100))
    identificada = base.withColumn(
        "conjunto",
        F.when(bucket < 70, "treino")
         .when(bucket < 85, "validacao")
         .otherwise("teste"),
    )
    treino = identificada.filter(F.col("conjunto") == "treino")
    validacao = identificada.filter(F.col("conjunto") == "validacao")
    teste = identificada.filter(F.col("conjunto") == "teste")
    return identificada, treino, validacao, teste


def escolher_limite(modelo, validacao: DataFrame):
    """Escolhe o limiar por F1 apenas na validação."""
    linhas = (
        modelo.transform(validacao)
        .select("label", vector_to_array("probability")[1].alias("score"))
        .collect()
    )
    if not linhas:
        raise ValueError("O conjunto de validação ficou vazio.")
    melhor = (-1.0, -1.0, 0.5)
    for limite in (i / 1000 for i in range(30, 301, 5)):
        vp = sum(r.label == 1.0 and r.score >= limite for r in linhas)
        fp = sum(r.label == 0.0 and r.score >= limite for r in linhas)
        fn = sum(r.label == 1.0 and r.score < limite for r in linhas)
        precisao = vp / (vp + fp) if vp + fp else 0.0
        recall = vp / (vp + fn) if vp + fn else 0.0
        f1 = 2 * precisao * recall / (precisao + recall) if precisao + recall else 0.0
        if (f1, precisao, limite) > melhor:
            melhor = (f1, precisao, limite)
    return melhor[2], melhor[0]


def gravar_propensao(spark, eventos: DataFrame):
    sessoes = construir_base_sessoes(eventos)
    features = construir_features(eventos)
    base = (
        sessoes.join(features, "session_id", "left")
        .fillna(0, subset=FEATURES)
    )
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features", handleInvalid="error")
    base_modelo = assembler.transform(base).cache()

    identificacao, treino, validacao, teste = dividir_por_visitante(base_modelo)
    if treino.filter(F.col("label") == 1.0).limit(1).count() == 0:
        raise ValueError("O conjunto de treino não contém sessões positivas.")
    if treino.filter(F.col("label") == 0.0).limit(1).count() == 0:
        raise ValueError("O conjunto de treino não contém sessões negativas.")
    if validacao.limit(1).count() == 0 or teste.limit(1).count() == 0:
        raise ValueError("A divisão por visitante produziu validação ou teste vazio.")

    rf = RandomForestClassifier(
        featuresCol="features", labelCol="label", predictionCol="prediction",
        probabilityCol="probability", rawPredictionCol="rawPrediction",
        numTrees=50, maxDepth=5, seed=SEED,
    )
    modelo = rf.fit(treino)
    limite, f1_validacao = escolher_limite(modelo, validacao)

    previsoes = (
        modelo.transform(base_modelo)
        # O conjunto não faz parte de base_modelo: é recuperado da divisão
        # determinística por visitante, usando session_id como chave.
        .join(
            identificacao.select("session_id", "conjunto"),
            on="session_id",
            how="inner",
        )
        .withColumn("probabilidade_compra", vector_to_array("probability")[1])
        .withColumn("previsao", (F.col("probabilidade_compra") >= F.lit(limite)).cast("integer"))
        .withColumn("limite_classificacao", F.lit(float(limite)))
        .select(
            "session_id", "visitante_id", "canal", "dispositivo",
            F.col("label").cast("integer").alias("comprou"),
            "probabilidade_compra", "previsao", "conjunto",
            "limite_classificacao", *FEATURES,
        )
    )
    previsoes.write.mode("overwrite").parquet(caminho("gold/propensao_sessao"))

    print("\nDIVISÃO DOS CONJUNTOS (por visitante):")
    identificacao.groupBy("conjunto").agg(
        F.count("*").alias("sessoes"),
        F.countDistinct("chave_visitante").alias("visitantes"),
    ).orderBy("conjunto").show(truncate=False)
    print(f"Limite escolhido na validação: {limite:.3f}")
    print(f"F1 na validação: {f1_validacao:.4f}")
    previsoes.groupBy("conjunto", "previsao").count().orderBy("conjunto", "previsao").show()
    previsoes.select("session_id", "comprou", "probabilidade_compra", "previsao", "conjunto").show(15, truncate=False)

    base_modelo.unpersist()
    return previsoes


def gravar_metricas(spark):
    previsoes = spark.read.parquet(caminho("gold/propensao_sessao"))
    treino = previsoes.filter(F.col("conjunto") == "treino")
    validacao = previsoes.filter(F.col("conjunto") == "validacao")
    teste = previsoes.filter(F.col("conjunto") == "teste")

    contagens = teste.agg(
        F.sum(((F.col("comprou") == 1) & (F.col("previsao") == 1)).cast("long")).alias("vp"),
        F.sum(((F.col("comprou") == 0) & (F.col("previsao") == 1)).cast("long")).alias("fp"),
        F.sum(((F.col("comprou") == 0) & (F.col("previsao") == 0)).cast("long")).alias("vn"),
        F.sum(((F.col("comprou") == 1) & (F.col("previsao") == 0)).cast("long")).alias("fn"),
    ).first()
    vp, fp, vn, fn = (int(contagens[x] or 0) for x in ("vp", "fp", "vn", "fn"))
    precisao = vp / (vp + fp) if vp + fp else 0.0
    recall = vp / (vp + fn) if vp + fn else 0.0
    f1 = 2 * precisao * recall / (precisao + recall) if precisao + recall else 0.0

    auc = BinaryClassificationEvaluator(
        labelCol="comprou", rawPredictionCol="probabilidade_compra",
        metricName="areaUnderROC",
    ).evaluate(teste)

    schema = StructType([
        StructField("algoritmo", StringType(), False),
        StructField("total_sessoes", LongType(), False),
        StructField("total_sessoes_treino", LongType(), False),
        StructField("total_sessoes_validacao", LongType(), False),
        StructField("total_sessoes_teste", LongType(), False),
        StructField("limite_classificacao", DoubleType(), False),
        StructField("f1_validacao", DoubleType(), False),
        StructField("verdadeiros_positivos", LongType(), False),
        StructField("falsos_positivos", LongType(), False),
        StructField("verdadeiros_negativos", LongType(), False),
        StructField("falsos_negativos", LongType(), False),
        StructField("auc_roc", DoubleType(), False),
        StructField("precisao", DoubleType(), False),
        StructField("recall", DoubleType(), False),
        StructField("f1", DoubleType(), False),
        StructField("features_utilizadas", StringType(), False),
    ])
    limite = float(teste.select("limite_classificacao").first()[0])
    # O F1 de validação é recalculado com o limiar selecionado, sem usar o teste.
    validacao_contagens = validacao.agg(
        F.sum(((F.col("comprou") == 1) & (F.col("previsao") == 1)).cast("long")).alias("vp"),
        F.sum(((F.col("comprou") == 0) & (F.col("previsao") == 1)).cast("long")).alias("fp"),
        F.sum(((F.col("comprou") == 1) & (F.col("previsao") == 0)).cast("long")).alias("fn"),
    ).first()
    vpv, fpv, fnv = (int(validacao_contagens[x] or 0) for x in ("vp", "fp", "fn"))
    pv = vpv / (vpv + fpv) if vpv + fpv else 0.0
    rv = vpv / (vpv + fnv) if vpv + fnv else 0.0
    f1_validacao = 2 * pv * rv / (pv + rv) if pv + rv else 0.0

    total_sessoes = previsoes.count()
    valores = [(
        "Random Forest", total_sessoes,
        treino.count(), validacao.count(), teste.count(), limite, float(f1_validacao),
        vp, fp, vn, fn, float(auc), float(precisao), float(recall), float(f1),
        ", ".join(FEATURES),
    )]
    resultado = spark.createDataFrame(valores, schema)
    resultado.write.mode("overwrite").parquet(caminho("gold/metricas_modelo"))

    print("\nMATRIZ DE CONFUSÃO — TESTE:")
    print(f"VP={vp} | FP={fp} | VN={vn} | FN={fn}")
    print("\nMÉTRICAS NO TESTE:")
    resultado.show(truncate=False)
    print(f"F1 na validação: {f1_validacao:.4f}")
    print(f"Total de sessões: {total_sessoes}")


def perfil_visitantes(eventos: DataFrame) -> DataFrame:
    """Resume as sessões em um perfil comportamental por visitante."""
    chave = F.coalesce("user_pseudo_id", "visitante_id")
    por_sessao = eventos.groupBy(chave.alias("visitante"), "session_id").agg(
        F.count("*").alias("eventos"),
        F.coalesce(F.max("percent_scrolled"), F.lit(0)).alias("scroll_max"),
        F.max((F.col("event_name") == "video_start").cast("int")).alias("viu_video"),
        F.max((F.col("event_name") == "generate_lead").cast("int")).alias("fez_lead"),
        F.max((F.col("event_name") == "purchase").cast("int")).alias("comprou"),
    )
    return por_sessao.groupBy("visitante").agg(
        F.count("*").cast("double").alias("sessoes"),
        F.avg("eventos").alias("eventos_medios"),
        F.avg("scroll_max").alias("scroll_medio"),
        F.avg("viu_video").alias("taxa_video"),
        F.max("fez_lead").cast("double").alias("fez_lead"),
        F.max("comprou").cast("double").alias("comprou"),
    )


def gravar_segmentos(spark, eventos: DataFrame):
    perfis = perfil_visitantes(eventos).filter(F.col("visitante").isNotNull())
    total = perfis.count()
    if total < 5:
        raise ValueError(f"Há apenas {total} visitantes; insuficiente para testar k=3,4,5.")

    perfis = perfis.orderBy("visitante").coalesce(1)
    vetor = VectorAssembler(inputCols=PERFIL, outputCol="bruto").transform(perfis)
    escalado = StandardScaler(inputCol="bruto", outputCol="features").fit(vetor).transform(vetor).cache()
    avaliador = ClusteringEvaluator(featuresCol="features", predictionCol="segmento")

    melhor = None
    for k in (3, 4, 5):
        modelo = KMeans(k=k, seed=SEED, featuresCol="features", predictionCol="segmento").fit(escalado)
        silhueta = avaliador.evaluate(modelo.transform(escalado))
        if melhor is None or silhueta > melhor[1]:
            melhor = (k, silhueta, modelo)

    k, silhueta, modelo = melhor
    segmentado = modelo.transform(escalado).select("visitante", "segmento", *PERFIL)
    perfil = segmentado.groupBy("segmento").agg(
        F.count("*").alias("visitantes_no_segmento"),
        *[F.round(F.avg(c), 3).alias(f"media_{c}") for c in PERFIL],
    )
    resultado = (
        segmentado.select("visitante", "segmento").join(perfil, "segmento")
        .withColumn("k", F.lit(k))
        .withColumn("silhueta", F.lit(round(float(silhueta), 4)))
    )
    resultado.coalesce(1).write.mode("overwrite").parquet(caminho("gold/segmentos_visitantes"))

    titulo("Segmentação de visitantes")
    print(f"Visitantes: {total}")
    print(f"Quantidade de segmentos escolhida (k): {k}")
    print(f"Silhueta: {silhueta:.4f}")
    print("\nResumo dos segmentos:")
    resultado.drop("visitante").distinct().orderBy("segmento").show(truncate=False)
    print("\nQuantidade de visitantes por segmento:")
    resultado.groupBy("segmento").agg(F.count("*").alias("visitantes")).orderBy("segmento").show()
    escalado.unpersist()


def main():
    spark = criar_sessao("gold_ml")
    try:
        exigir_camada(spark, glob_eventos("silver"), "python pipeline/02_silver_eventos.py")
        eventos = ler_eventos(spark, "silver").cache()
        try:
            titulo("Propensão de compra por sessão — Random Forest")
            previsoes = gravar_propensao(spark, eventos)
            titulo("Métricas do modelo — conjunto de teste")
            gravar_metricas(spark)
            titulo("Segmentação de visitantes — K-Means")
            gravar_segmentos(spark, eventos)
            titulo("Job gold_ml concluído com sucesso")
        finally:
            eventos.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
