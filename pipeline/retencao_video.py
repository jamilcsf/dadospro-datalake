
from pyspark.sql import functions as F

from comum import (
    criar_sessao,
    caminho,
    ler_eventos,
    titulo,
)

# Inicia a sessão Spark.
spark = criar_sessao("gold_retencao_video")

# Lê os eventos válidos da Silver.
silver = ler_eventos(spark, "silver")

# Identifica as sessões que iniciaram a reprodução
# e o maior percentual alcançado em cada sessão.
videos = (
    silver
    .filter(
        F.col("event_name").isin(
            "video_start",
            "video_progress",
            "video_complete",
        )
    )
    .groupBy("session_id", "params.video_title")
    .agg(
        F.max(
            F.when(
                F.col("event_name") == "video_start", 1
            ).otherwise(0)
        ).alias("iniciou_video"),
        F.max("params.video_percent").alias("percentual_maximo"),
    )
    .filter(F.col("iniciou_video") == 1)
)

# Calcula quantas sessões chegaram a cada marco.
retencao = (
    videos
    .groupBy("video_title")
    .agg(
        F.countDistinct("session_id").alias("sessoes_com_video_start"),
        F.countDistinct(
            F.when(F.col("percentual_maximo") >= 10, F.col("session_id"))
        ).alias("sessoes_10"),
        F.countDistinct(
            F.when(F.col("percentual_maximo") >= 25, F.col("session_id"))
        ).alias("sessoes_25"),
        F.countDistinct(
            F.when(F.col("percentual_maximo") >= 50, F.col("session_id"))
        ).alias("sessoes_50"),
        F.countDistinct(
            F.when(F.col("percentual_maximo") >= 75, F.col("session_id"))
        ).alias("sessoes_75"),
        F.countDistinct(
            F.when(F.col("percentual_maximo") >= 100, F.col("session_id"))
        ).alias("sessoes_100"),
    )
)

# Transforma as quantidades em percentuais de retenção.
for marco in [10, 25, 50, 75, 100]:
    retencao = retencao.withColumn(
        f"retencao_{marco}",
        F.round(
            F.col(f"sessoes_{marco}")
            / F.col("sessoes_com_video_start") * 100,
            2,
        ),
    )

# Grava a tabela Gold.
retencao.write.mode("overwrite").parquet(
    caminho("gold/retencao_video")
)

titulo("Retenção de vídeo - Gold")

retencao.show(truncate=False)

print("Tabela gold/retencao_video gravada com sucesso.")

spark.stop()
