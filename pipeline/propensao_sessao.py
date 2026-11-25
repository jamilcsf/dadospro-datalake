
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.functions import vector_to_array

from comum import criar_sessao, ler_eventos, caminho, titulo


spark = criar_sessao("propensao_sessao")

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
    "visualizacoes_lista"
]

# Ler eventos Silver
eventos = ler_eventos(spark, "silver")

# Encontrar o primeiro add_to_cart de cada sessão
primeiro_carrinho = (
    eventos
    .filter(F.col("event_name") == "add_to_cart")
    .groupBy("session_id")
    .agg(
        F.min("event_ts").alias("primeiro_add_to_cart")
    )
)

# Considerar somente eventos da landing page
landing = eventos.filter(
    F.col("page.location") == "https://loja.exemplo/"
)

# Usar somente eventos anteriores ao primeiro carrinho.
# Se não houver carrinho, considerar todos os eventos da landing.
comportamento = (
    landing
    .join(primeiro_carrinho, on="session_id", how="left")
    .filter(
        F.col("primeiro_add_to_cart").isNull() |
        (F.col("event_ts") < F.col("primeiro_add_to_cart"))
    )
)

# Criar variáveis comportamentais por sessão
agregado = (
    comportamento
    .groupBy("session_id")
    .agg(
        F.sum(
            F.when(F.col("event_name") == "page_view", 1).otherwise(0)
        ).alias("visualizacoes_pagina"),

        F.max(
            F.when(F.col("event_name") == "scroll",
                   F.col("percent_scrolled"))
        ).alias("scroll_maximo_pre_carrinho"),

        F.max(
            F.when(F.col("event_name").isin(
                "video_start", "video_progress", "video_complete"
            ), F.col("video_percent"))
        ).alias("video_maximo_pre_carrinho"),

        F.sum(
            F.when(F.col("event_name") == "video_start", 1).otherwise(0)
        ).alias("inicios_video"),

        F.sum(
            F.when(F.col("event_name") == "site_search", 1).otherwise(0)
        ).alias("buscas"),

        F.sum(
            F.when(
                (F.col("event_name") == "site_search") &
                (F.col("search_results") == 0), 1
            ).otherwise(0)
        ).alias("buscas_sem_resultado"),

        F.sum(
            F.when(F.col("event_name") == "click", 1).otherwise(0)
        ).alias("cliques"),

        F.sum(
            F.when(
                (F.col("event_name") == "click") &
                (F.col("outbound") == True), 1
            ).otherwise(0)
        ).alias("cliques_externos"),

        F.sum(
            F.when(F.col("event_name") == "file_download", 1).otherwise(0)
        ).alias("downloads"),

        F.sum(
            F.when(F.col("event_name") == "generate_lead", 1).otherwise(0)
        ).alias("leads"),

        F.sum(
            F.when(F.col("event_name") == "select_item", 1).otherwise(0)
        ).alias("itens_selecionados"),

        F.sum(
            F.when(F.col("event_name") == "view_item", 1).otherwise(0)
        ).alias("visualizacoes_produto"),

        F.sum(
            F.when(F.col("event_name") == "view_item_list", 1).otherwise(0)
        ).alias("visualizacoes_lista")
    )
)

# Todas as sessões entram, mesmo sem eventos na landing
sessoes = spark.read.parquet(caminho("gold/sessoes"))

base = (
    sessoes
    .select(
        "session_id",
        "visitante_id",
        "canal",
        "dispositivo",
        F.col("comprou").cast("double").alias("label")
    )
    .join(agregado, on="session_id", how="left")
    .fillna(0, subset=FEATURES)
)

# Montar vetor de variáveis
assembler = VectorAssembler(
    inputCols=FEATURES,
    outputCol="features",
    handleInvalid="keep"
)

base_modelo = assembler.transform(base)

# Separar treinamento, validação e teste
treino, validacao, teste = base_modelo.randomSplit(
    [0.70, 0.15, 0.15],
    seed=42
)

# Treinar o modelo somente com o conjunto de treinamento
modelo = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    predictionCol="prediction",
    probabilityCol="probability",
    numTrees=50,
    maxDepth=5,
    seed=42
)

modelo_treinado = modelo.fit(treino)

# Gerar probabilidades no conjunto de validação
probabilidades_validacao = (
    modelo_treinado
    .transform(validacao)
    .select(
        "label",
        vector_to_array("probability")[1].alias("probabilidade_compra")
    )
    .collect()
)

# Escolher o limite que maximiza o F1 na validação
melhor_f1 = -1.0
melhor_precisao = -1.0
limite_escolhido = 0.5

for limite in [i / 1000 for i in range(30, 301, 5)]:

    vp = sum(
        1 for r in probabilidades_validacao
        if r["label"] == 1.0 and r["probabilidade_compra"] >= limite
    )

    fp = sum(
        1 for r in probabilidades_validacao
        if r["label"] == 0.0 and r["probabilidade_compra"] >= limite
    )

    fn = sum(
        1 for r in probabilidades_validacao
        if r["label"] == 1.0 and r["probabilidade_compra"] < limite
    )

    precisao = vp / (vp + fp) if (vp + fp) else 0.0
    recall = vp / (vp + fn) if (vp + fn) else 0.0

    f1 = (
        2 * precisao * recall / (precisao + recall)
        if (precisao + recall) else 0.0
    )

    if (f1, precisao, limite) > (
        melhor_f1, melhor_precisao, limite_escolhido
    ):
        melhor_f1 = f1
        melhor_precisao = precisao
        limite_escolhido = limite

print("\nSELECAO DO LIMITE NA VALIDACAO:")
print(f"Sessoes de validacao: {len(probabilidades_validacao)}")
print(f"Limite escolhido: {limite_escolhido:.3f}")
print(f"F1 na validacao: {melhor_f1:.4f}")

# Identificar a qual conjunto cada sessão pertence
identificacao = (
    treino.select("session_id")
    .withColumn("conjunto", F.lit("treino"))
    .unionByName(
        validacao.select("session_id")
        .withColumn("conjunto", F.lit("validacao"))
    )
    .unionByName(
        teste.select("session_id")
        .withColumn("conjunto", F.lit("teste"))
    )
)

# Gerar probabilidades e classificação para todas as sessões
previsoes = (
    modelo_treinado
    .transform(base_modelo)
    .withColumn(
        "probabilidade_compra",
        vector_to_array("probability")[1]
    )
    .join(identificacao, on="session_id", how="inner")
    .withColumn(
        "previsao",
        F.when(
            F.col("probabilidade_compra") >= limite_escolhido, 1
        ).otherwise(0)
    )
    .withColumn(
        "limite_classificacao",
        F.lit(limite_escolhido)
    )
    .select(
        "session_id",
        "visitante_id",
        "canal",
        "dispositivo",
        F.col("label").cast("integer").alias("comprou"),
        "probabilidade_compra",
        "previsao",
        "conjunto",
        "limite_classificacao",
        *FEATURES
    )
)

# Gravar tabela Gold atualizada
previsoes.write.mode("overwrite").parquet(
    caminho("gold/propensao_sessao")
)

print("\nDISTRIBUICAO DOS CONJUNTOS:")
identificacao.groupBy("conjunto").count().orderBy("conjunto").show()

print("\nDISTRIBUICAO DAS PREVISOES:")
previsoes.groupBy("conjunto", "previsao").count().orderBy(
    "conjunto", "previsao"
).show()

print("\nAMOSTRA DAS PREVISOES:")
previsoes.select(
    "session_id",
    "comprou",
    "probabilidade_compra",
    "previsao",
    "conjunto"
).show(15, truncate=False)

print("\nTOTAL DE SESSOES:", previsoes.count())

titulo("Tabela gold/propensao_sessao gravada com sucesso.")

spark.stop()
