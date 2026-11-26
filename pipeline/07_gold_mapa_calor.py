"""Gold: mapas de calor de cliques e rolagem, a partir da Silver."""
from pyspark.sql import Window, functions as F
from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo

MARCOS = [0, 25, 50, 75, 90]
LANDING = "/"


def pagina_do_evento(coluna="page_location"):
    url = F.trim(F.col(coluna))
    sem_dominio = F.regexp_replace(url, r"(?i)^(?:[a-z][a-z0-9+.-]*:)?//[^/?#]+", "")
    pagina = F.regexp_replace(sem_dominio, r"[?#].*$", "")
    return (F.when(url.isNull() | (url == ""), F.lit(None).cast("string"))
            .when(pagina == "", F.lit("/"))
            .when(pagina.startswith("/"), pagina)
            .otherwise(F.concat(F.lit("/"), pagina)))


def mapa_cliques(eventos):
    cliques = (eventos.filter(F.col("element_id").isNotNull())
               .groupBy("pagina", "element_id")
               .agg(F.count("*").alias("cliques"),
                    F.countDistinct("session_id").alias("sessoes")))
    visitas = (eventos.filter(F.col("event_name") == "page_view")
               .groupBy("pagina")
               .agg(F.countDistinct("session_id").alias("sessoes_na_pagina")))
    maximo = F.max("cliques").over(Window.partitionBy("pagina"))
    base = F.col("sessoes_na_pagina")
    return (cliques.join(visitas, "pagina", "left")
            .withColumn("sessoes_na_pagina", F.coalesce(base, F.lit(0)).cast("long"))
            .withColumn("taxa_clique", F.when(base > 0, F.round(F.col("sessoes") / base, 4)))
            .withColumn("intensidade", F.round(F.col("cliques") / maximo, 4))
            .select("pagina", "element_id", "cliques", "sessoes", "sessoes_na_pagina", "taxa_clique", "intensidade"))


def mapa_rolagem(eventos):
    landing = eventos.filter(F.col("pagina") == LANDING)
    base = landing.filter(F.col("event_name") == "page_view").select("session_id").distinct()
    maximo = (landing.filter(F.col("event_name") == "scroll")
              .groupBy("session_id").agg(F.max("percent_scrolled").alias("maximo")))
    por_sessao = base.join(maximo, "session_id", "left").fillna(0, ["maximo"])
    somas = por_sessao.agg(*[
        F.coalesce(F.sum((F.col("maximo") >= marco).cast("long")), F.lit(0)).alias(f"m{marco}")
        for marco in MARCOS])
    pilha = ", ".join(f"{marco}, m{marco}" for marco in MARCOS)
    return (somas.select(F.col("m0").alias("total"), F.expr(f"stack({len(MARCOS)}, {pilha}) as (marco, sessoes_que_chegaram)"))
            .select(F.lit(LANDING).alias("pagina"), F.col("marco").cast("int"),
                    F.col("sessoes_que_chegaram").cast("long"),
                    F.when(F.col("total") > 0, F.round(F.col("sessoes_que_chegaram") / F.col("total"), 4)).alias("pct_sessoes")))


def main():
    spark = criar_sessao("gold-mapa-calor")
    eventos = None
    try:
        exigir_camada(spark, glob_eventos("silver"), "python pipeline/02_silver_eventos.py")
        eventos = (ler_eventos(spark, "silver", "parquet")
                   .withColumn("pagina", pagina_do_evento())
                   .filter(F.col("pagina").isNotNull()).cache())
        for nome, resultado, ordem in [
            ("mapa_calor_cliques", mapa_cliques(eventos), [F.col("pagina"), F.desc("cliques"), F.col("element_id")]),
            ("mapa_calor_rolagem", mapa_rolagem(eventos), [F.col("marco")]),
        ]:
            destino = caminho(f"gold/{nome}")
            resultado.coalesce(1).write.mode("overwrite").parquet(destino)
            titulo(f"Gold — {nome}")
            gravado = spark.read.parquet(destino)
            gravado.orderBy(*ordem).show(40, truncate=False)
            print(f"Tabela {nome} gravada: {gravado.count()} linhas")
        print("Job gold_mapa_calor concluído com sucesso.")
    finally:
        if eventos is not None:
            eventos.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
