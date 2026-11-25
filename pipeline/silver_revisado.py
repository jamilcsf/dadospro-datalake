from pyspark.sql import functions as F
from pyspark.sql.window import Window

from comum import (
    criar_sessao,
    exigir_camada,
    glob_eventos,
    ler_eventos,
    gravar_eventos,
    caminho,
    titulo,
)


# Inicia a sessão Spark.
spark = criar_sessao("silver")

# Verifica se a camada Bronze existe.
exigir_camada(
    spark,
    glob_eventos("bronze"),
    "python pipeline/bronze.py",
)

# Lê os eventos da Bronze.
bronze = ler_eventos(spark, "bronze")

# Diagnóstico inicial.
total = bronze.count()

ids_nulos = bronze.filter(
    F.col("event_id").isNull()
).count()

duplicados = (
    bronze
    .filter(F.col("event_id").isNotNull())
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .agg(F.sum(F.col("count") - 1).alias("total"))
    .first()["total"]
)

duplicados = duplicados or 0

# Catálogo de eventos permitidos.
EVENTOS_VALIDOS = [
    "page_view", "scroll", "click", "site_search",
    "video_start", "video_progress", "video_complete",
    "file_download", "view_item_list", "select_item",
    "view_item", "add_to_cart", "remove_from_cart",
    "view_cart", "begin_checkout", "add_shipping_info",
    "add_payment_info", "purchase", "refund",
    "generate_lead", "sign_up", "login",
]

# R5: remove duplicatas de forma determinística.
# Em caso de repetição do event_id, mantém a primeira ingestão.
janela_deduplicacao = (
    Window
    .partitionBy("event_id")
    .orderBy(
        F.col("instante_ingestao").asc_nulls_last(),
        F.col("arquivo_origem").asc_nulls_last(),
    )
)

silver = (
    bronze
    .withColumn(
        "_ordem_deduplicacao",
        F.row_number().over(janela_deduplicacao),
    )
    .filter(F.col("_ordem_deduplicacao") == 1)
    .drop("_ordem_deduplicacao")
)

total_silver = silver.count()

# Converte a data/hora usando o formato ISO 8601 do gerador.
# Mantém event_ts para compatibilidade com os demais scripts
# e cria ts, nome utilizado na solução de referência.
silver = (
    silver
    .withColumn(
        "ts",
        F.to_timestamp(
            F.col("event_ts"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        ),
    )
    .withColumn("event_ts", F.col("ts"))
)

# R6: extrai campos aninhados e parâmetros para colunas próprias.
# As estruturas originais também são preservadas para compatibilidade.
silver = silver.select(
    "*",
    F.col("page.location").alias("page_location"),
    F.col("page.referrer").alias("page_referrer"),
    F.col("traffic.source").alias("source"),
    F.col("traffic.medium").alias("medium"),
    F.col("traffic.campaign").alias("campaign"),
    F.col("device.category").alias("device_category"),
    F.col("device.os").alias("device_os"),
    F.col("device.browser").alias("device_browser"),
    F.col("params.coupon").cast("string").alias("coupon"),
    F.col("params.currency").cast("string").alias("currency"),
    F.col("params.element_id").cast("string").alias("element_id"),
    F.col("params.file_extension").cast("string").alias("file_extension"),
    F.col("params.file_name").cast("string").alias("file_name"),
    F.col("params.item_list_id").cast("string").alias("item_list_id"),
    F.col("params.item_list_name").cast("string").alias("item_list_name"),
    F.col("params.lead_source").cast("string").alias("lead_source"),
    F.col("params.link_domain").cast("string").alias("link_domain"),
    F.col("params.link_id").cast("string").alias("link_id"),
    F.col("params.link_text").cast("string").alias("link_text"),
    F.col("params.link_url").cast("string").alias("link_url"),
    F.col("params.method").cast("string").alias("method"),
    F.col("params.outbound").cast("boolean").alias("outbound"),
    F.col("params.page_title").cast("string").alias("page_title"),
    F.col("params.payment_type").cast("string").alias("payment_type"),
    F.col("params.percent_scrolled").cast("int").alias("percent_scrolled"),
    F.col("params.reason").cast("string").alias("reason"),
    F.col("params.search_area").cast("string").alias("search_area"),
    F.col("params.search_results").cast("int").alias("search_results"),
    F.col("params.search_term").cast("string").alias("search_term"),
    F.col("params.shipping").cast("double").alias("shipping"),
    F.col("params.shipping_tier").cast("string").alias("shipping_tier"),
    F.col("params.tax").cast("double").alias("tax"),
    F.col("params.transaction_id").cast("string").alias("transaction_id"),
    F.col("params.value").cast("double").alias("value"),
    F.col("params.video_current_time").cast("int").alias("video_current_time"),
    F.col("params.video_duration").cast("int").alias("video_duration"),
    F.col("params.video_percent").cast("int").alias("video_percent"),
    F.col("params.video_provider").cast("string").alias("video_provider"),
    F.col("params.video_title").cast("string").alias("video_title"),
)

# R7: identifica eventos inválidos, mantendo a prioridade das regras.
silver = silver.withColumn(
    "motivo_rejeicao",
    F.when(
        F.col("event_name").isNull()
        | (~F.col("event_name").isin(EVENTOS_VALIDOS)),
        "event_name fora do catálogo",
    )
    .when(
        F.col("ts").isNull(),
        "event_ts nulo ou inválido",
    )
    .when(
        F.col("ts") > F.col("instante_ingestao"),
        "event_ts posterior à ingestão",
    )
    .when(
        F.col("event_name").isin("purchase", "refund")
        & (
            F.col("transaction_id").isNull()
            | (F.trim(F.col("transaction_id")) == "")
        ),
        "transaction_id ausente",
    )
    .when(
        F.col("value").isNotNull()
        & (
            (F.col("value") < 0)
            | F.col("currency").isNull()
            | (F.col("currency") != "BRL")
        ),
        "value negativo ou currency diferente de BRL",
    )
    .when(
        (F.col("event_name") == "scroll")
        & (
            F.col("percent_scrolled").isNull()
            | (~F.col("percent_scrolled").isin(25, 50, 75, 90))
        ),
        "percent_scrolled inválido",
    )
    .when(
        F.col("event_name").isin(
            "video_start", "video_progress", "video_complete"
        )
        & (
            F.col("video_percent").isNull()
            | (~F.col("video_percent").isin(0, 10, 25, 50, 75, 100))
        ),
        "video_percent inválido",
    )
    .otherwise(None),
)

# Verifica se os reembolsos possuem compras válidas.
compras_validas = (
    silver
    .filter(
        (F.col("event_name") == "purchase")
        & F.col("motivo_rejeicao").isNull()
    )
    .select("transaction_id")
    .distinct()
    .withColumn("_compra_encontrada", F.lit(True))
)

silver = (
    silver
    .join(compras_validas, on="transaction_id", how="left")
    .withColumn(
        "motivo_rejeicao",
        F.when(
            (F.col("event_name") == "refund")
            & F.col("motivo_rejeicao").isNull()
            & F.col("_compra_encontrada").isNull(),
            "refund sem purchase válido correspondente",
        ).otherwise(F.col("motivo_rejeicao")),
    )
    .drop("_compra_encontrada")
)

# Separa eventos aceitos e rejeitados.
rejeitados = silver.filter(
    F.col("motivo_rejeicao").isNotNull()
)

silver = silver.filter(
    F.col("motivo_rejeicao").isNull()
).drop("motivo_rejeicao")

# R9: identity stitching.
# Para cada user_pseudo_id, recupera o user_id mais recente
# e o propaga também aos eventos anteriores ao cadastro/login.
janela_identidade = (
    Window
    .partitionBy("user_pseudo_id")
    .orderBy(
        F.col("ts").desc(),
        F.col("event_id").desc(),
    )
    .rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing,
    )
)

identidades = (
    silver
    .filter(F.col("user_pseudo_id").isNotNull())
    .withColumn(
        "user_id_resolvido",
        F.first("user_id", ignorenulls=True).over(janela_identidade),
    )
    .select("user_pseudo_id", "user_id_resolvido")
    .dropDuplicates(["user_pseudo_id"])
)

silver = (
    silver
    .join(identidades, on="user_pseudo_id", how="left")
    .withColumn(
        "visitante_id",
        F.coalesce(
            F.col("user_id_resolvido"),
            F.col("user_id"),
            F.col("user_pseudo_id"),
            F.col("event_id"),
        ),
    )
)

# R8: sessionização por user_pseudo_id, não por visitante_id.
# Eventos sem user_pseudo_id recebem uma chave individual para não
# serem agrupados indevidamente em uma única sessão de valores nulos.
silver = silver.withColumn(
    "_chave_sessao",
    F.coalesce(
        F.col("user_pseudo_id"),
        F.concat(F.lit("evento_"), F.col("event_id")),
    ),
)

janela_sessao = (
    Window
    .partitionBy("_chave_sessao")
    .orderBy("ts", "event_id")
)

silver = silver.withColumn(
    "evento_anterior",
    F.lag("ts").over(janela_sessao),
)

silver = silver.withColumn(
    "intervalo_segundos",
    F.col("ts").cast("long")
    - F.col("evento_anterior").cast("long"),
)

silver = silver.withColumn(
    "nova_sessao",
    F.when(
        F.col("evento_anterior").isNull()
        | (F.col("intervalo_segundos") > 1800),
        1,
    ).otherwise(0),
)

janela_acumulada = janela_sessao.rowsBetween(
    Window.unboundedPreceding,
    Window.currentRow,
)

silver = silver.withColumn(
    "numero_sessao",
    F.sum("nova_sessao").over(janela_acumulada),
)

# Mantém o padrão pseudo_id-1 esperado na solução de referência.
silver = silver.withColumn(
    "session_id",
    F.concat_ws(
        "-",
        F.col("_chave_sessao"),
        F.col("numero_sessao").cast("string"),
    ),
).drop(
    "_chave_sessao",
    "evento_anterior",
    "intervalo_segundos",
    "nova_sessao",
)

# R10: transforma cada item do evento em um registro individual,
# preservando também sua posição original no array.
silver_itens = (
    silver
    .filter(
        F.col("items").isNotNull()
        & (F.size("items") > 0)
    )
    .select(
        "*",
        F.posexplode("items").alias("posicao", "item"),
    )
    .select(
        "event_id",
        "event_name",
        "ts",
        "event_ts",
        "visitante_id",
        "session_id",
        "user_id",
        "user_id_resolvido",
        "user_pseudo_id",
        "transaction_id",
        "year",
        "month",
        "day",
        "posicao",
        F.col("item.index").alias("item_index"),
        F.col("item.item_category").alias("item_category"),
        F.col("item.item_id").alias("item_id"),
        F.col("item.item_list_id").alias("item_list_id"),
        F.col("item.item_list_name").alias("item_list_name"),
        F.col("item.item_name").alias("item_name"),
        F.col("item.price").cast("double").alias("price"),
        F.col("item.quantity").cast("int").alias("quantity"),
    )
)

# Diagnóstico final.
total_aceitos = silver.count()
total_rejeitados = rejeitados.count()

titulo("Validação e transformação - Silver")

print(f"Registros na Bronze: {total}")
print(f"IDs nulos: {ids_nulos}")
print(f"Duplicatas excedentes: {duplicados}")
print(f"Registros após deduplicação: {total_silver}")
print(f"Eventos aceitos: {total_aceitos}")
print(f"Eventos rejeitados: {total_rejeitados}")
print(
    "Sessões identificadas: "
    f"{silver.select('session_id').distinct().count()}"
)

print("\nRejeições por motivo:")
(
    rejeitados
    .groupBy("motivo_rejeicao")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)

# Grava a Silver e os itens usando o padrão de particionamento do projeto.
gravar_eventos(silver, "silver", "overwrite")
gravar_eventos(silver_itens, "silver/itens", "overwrite")

# Grava os rejeitados no diretório exigido pela solução de referência.
# A gravação direta evita o particionamento adicional por event_name.
(
    rejeitados
    .write
    .mode("overwrite")
    .parquet(caminho("silver/eventos_rejeitados"))
)

titulo("Silver concluída")

spark.stop()
