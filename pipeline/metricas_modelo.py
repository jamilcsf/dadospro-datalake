
from pyspark.sql import functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType
)

from comum import criar_sessao, caminho, titulo


spark = criar_sessao("metricas_modelo")

previsoes = spark.read.parquet(
    caminho("gold/propensao_sessao")
)

# Separar os conjuntos
treino = previsoes.filter(F.col("conjunto") == "treino")
validacao = previsoes.filter(F.col("conjunto") == "validacao")
teste = (
    previsoes
    .filter(F.col("conjunto") == "teste")
    .withColumn("comprou", F.col("comprou").cast("double"))
)

total_treino = treino.count()
total_validacao = validacao.count()
total_teste = teste.count()
total_sessoes = previsoes.count()

if total_teste == 0:
    raise ValueError("O conjunto de teste está vazio.")

# Recuperar o limite escolhido na validação
limite = float(
    teste.select("limite_classificacao").first()[0]
)

# Calcular F1 da validação para registro
validacao_metricas = validacao.agg(
    F.sum(
        F.when(
            (F.col("comprou") == 1) & (F.col("previsao") == 1), 1
        ).otherwise(0)
    ).alias("vp"),

    F.sum(
        F.when(
            (F.col("comprou") == 0) & (F.col("previsao") == 1), 1
        ).otherwise(0)
    ).alias("fp"),

    F.sum(
        F.when(
            (F.col("comprou") == 1) & (F.col("previsao") == 0), 1
        ).otherwise(0)
    ).alias("fn")
).first()

vp_val = int(validacao_metricas["vp"] or 0)
fp_val = int(validacao_metricas["fp"] or 0)
fn_val = int(validacao_metricas["fn"] or 0)

precisao_val = (
    vp_val / (vp_val + fp_val)
    if (vp_val + fp_val) else 0.0
)

recall_val = (
    vp_val / (vp_val + fn_val)
    if (vp_val + fn_val) else 0.0
)

f1_validacao = (
    2 * precisao_val * recall_val /
    (precisao_val + recall_val)
    if (precisao_val + recall_val) else 0.0
)

# Matriz de confusão exclusivamente no teste
contagens = teste.agg(
    F.sum(
        F.when(
            (F.col("comprou") == 1) &
            (F.col("previsao") == 1), 1
        ).otherwise(0)
    ).alias("verdadeiros_positivos"),

    F.sum(
        F.when(
            (F.col("comprou") == 0) &
            (F.col("previsao") == 1), 1
        ).otherwise(0)
    ).alias("falsos_positivos"),

    F.sum(
        F.when(
            (F.col("comprou") == 0) &
            (F.col("previsao") == 0), 1
        ).otherwise(0)
    ).alias("verdadeiros_negativos"),

    F.sum(
        F.when(
            (F.col("comprou") == 1) &
            (F.col("previsao") == 0), 1
        ).otherwise(0)
    ).alias("falsos_negativos")
).first()

vp = int(contagens["verdadeiros_positivos"] or 0)
fp = int(contagens["falsos_positivos"] or 0)
vn = int(contagens["verdadeiros_negativos"] or 0)
fn = int(contagens["falsos_negativos"] or 0)

precisao = vp / (vp + fp) if (vp + fp) else 0.0
recall = vp / (vp + fn) if (vp + fn) else 0.0

f1 = (
    2 * precisao * recall / (precisao + recall)
    if (precisao + recall) else 0.0
)

# AUC-ROC no conjunto de teste
avaliador = BinaryClassificationEvaluator(
    labelCol="comprou",
    rawPredictionCol="probabilidade_compra",
    metricName="areaUnderROC"
)

auc = avaliador.evaluate(teste)

# Variáveis utilizadas pelo modelo
features = [
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

# Estrutura da tabela Gold
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
    StructField("features_utilizadas", StringType(), False)
])

resultado = spark.createDataFrame(
    [(
        "Random Forest",
        total_sessoes,
        total_treino,
        total_validacao,
        total_teste,
        limite,
        float(f1_validacao),
        vp,
        fp,
        vn,
        fn,
        float(auc),
        float(precisao),
        float(recall),
        float(f1),
        ", ".join(features)
    )],
    schema=schema
)

# Gravar tabela Gold
resultado.write.mode("overwrite").parquet(
    caminho("gold/metricas_modelo")
)

print("\nDIVISAO DOS CONJUNTOS:")
print(f"Treino: {total_treino}")
print(f"Validacao: {total_validacao}")
print(f"Teste: {total_teste}")
print(f"Total: {total_sessoes}")

print("\nMATRIZ DE CONFUSAO - TESTE:")
print(f"Verdadeiros positivos: {vp}")
print(f"Falsos positivos: {fp}")
print(f"Verdadeiros negativos: {vn}")
print(f"Falsos negativos: {fn}")

print("\nMETRICAS DO MODELO:")
resultado.show(truncate=False)

titulo("Tabela gold/metricas_modelo gravada com sucesso.")

spark.stop()
