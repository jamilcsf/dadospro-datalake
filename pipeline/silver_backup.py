
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from comum import (
    criar_sessao,
    exigir_camada,
    glob_eventos,
    ler_eventos,
    gravar_eventos,
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

# R5: remove eventos duplicados.
silver = bronze.dropDuplicates(["event_id"])

total_silver = silver.count()

# Converte o horário para timestamp.
silver = silver.withColumn(
    "event_ts",
    F.to_timestamp("event_ts"),
)

# R6: extrai os parâmetros para colunas próprias.
silver = silver.select(
    "*",
    F.col("params.coupon").alias("coupon"),
    F.col("params.currency").alias("currency"),
    F.col("params.element_id").alias("element_id"),
    F.col("params.file_extension").alias("file_extension"),
    F.col("params.file_name").alias("file_name"),
    F.col("params.item_list_id").alias("item_list_id"),
    F.col("params.item_list_name").alias("item_list_name"),
    F.col("params.lead_source").alias("lead_source"),
    F.col("params.link_domain").alias("link_domain"),
    F.col("params.link_id").alias("link_id"),
    F.col("params.link_text").alias("link_text"),
    F.col("params.link_url").alias("link_url"),
    F.col("params.method").alias("method"),
    F.col("params.outbound").alias("outbound"),
    F.col("params.page_title").alias("page_title"),
    F.col("params.payment_type").alias("payment_type"),
    F.col("params.percent_scrolled").alias("percent_scrolled"),
    F.col("params.reason").alias("reason"),
    F.col("params.search_area").alias("search_area"),
    F.col("params.search_results").alias("search_results"),
    F.col("params.search_term").alias("search_term"),
    F.col("params.shipping").alias("shipping"),
    F.col("params.shipping_tier").alias("shipping_tier"),
    F.col("params.tax").alias("tax"),
    F.col("params.transaction_id").alias("transaction_id"),
    F.col("params.value").alias("value"),
    F.col("params.video_current_time").alias("video_current_time"),
    F.col("params.video_duration").alias("video_duration"),
    F.col("params.video_percent").alias("video_percent"),
    F.col("params.video_provider").alias("video_provider"),
    F.col("params.video_title").alias("video_title"),
)

# R7: identifica eventos inválidos.
silver = silver.withColumn(
    "motivo_rejeicao",
    F.when(
        F.col("event_name").isNull()
        | (~F.col("event_name").isin(EVENTOS_VALIDOS)),
        "event_name fora do catálogo",
    )
    .when(
        F.col("event_ts").isNull(),
        "event_ts nulo ou inválido",
    )
    .when(
        F.col("event_ts") > F.col("instante_ingestao"),
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
# Relaciona o identificador anônimo ao usuário identificado.
janela_identidade = (
    Window
    .partitionBy("user_pseudo_id")
    .orderBy("event_ts", "event_id")
)

identidades = (
    silver
    .filter(
        F.col("user_pseudo_id").isNotNull()
        & F.col("user_id").isNotNull()
    )
    .withColumn(
        "_ordem_identidade",
        F.row_number().over(janela_identidade),
    )
    .filter(F.col("_ordem_identidade") == 1)
    .select(
        "user_pseudo_id",
        F.col("user_id").alias("user_id_resolvido"),
    )
)

silver = (
    silver
    .join(identidades, on="user_pseudo_id", how="left")
    .withColumn(
        "visitante_id",
        F.coalesce(
            F.col("user_id"),
            F.col("user_id_resolvido"),
            F.col("user_pseudo_id"),
            F.col("event_id"),
        ),
    )
    .drop("user_id_resolvido")
)

# R8: sessionização.
# Ordena os eventos de cada visitante.
janela_visitante = (
    Window
    .partitionBy("visitante_id")
    .orderBy("event_ts", "event_id")
)

# Recupera o horário do evento anterior.
silver = silver.withColumn(
    "evento_anterior",
    F.lag("event_ts").over(janela_visitante),
)

# Calcula o intervalo entre eventos.
silver = silver.withColumn(
    "intervalo_segundos",
    F.col("event_ts").cast("long")
    - F.col("evento_anterior").cast("long"),
)

# Inicia nova sessão quando o intervalo supera 30 minutos.
silver = silver.withColumn(
    "nova_sessao",
    F.when(
        F.col("evento_anterior").isNull()
        | (F.col("intervalo_segundos") > 1800),
        1,
    ).otherwise(0),
)

# Numera as sessões de cada visitante.
janela_acumulada = janela_visitante.rowsBetween(
    Window.unboundedPreceding,
    Window.currentRow,
)

silver = silver.withColumn(
    "numero_sessao",
    F.sum("nova_sessao").over(janela_acumulada),
)

# Cria identificador da sessão.
silver = silver.withColumn(
    "session_id",
    F.concat_ws(
        "_",
        F.col("visitante_id"),
        F.col("numero_sessao").cast("string"),
    ),
)


# R10: transforma cada item do evento em um registro individual.
silver_itens = (
    silver
    .filter(
        F.col("items").isNotNull()
        & (F.size("items") > 0)
    )
    .withColumn(
        "item",
        F.explode("items"),
    )
    .select(
        "event_id",
        "event_name",
        "event_ts",
        "visitante_id",
        "session_id",
        "user_id",
        "user_pseudo_id",
        "transaction_id",
        "year",
        "month",
        "day",
        F.col("item.index").alias("item_index"),
        F.col("item.item_category").alias("item_category"),
        F.col("item.item_id").alias("item_id"),
        F.col("item.item_list_id").alias("item_list_id"),
        F.col("item.item_list_name").alias("item_list_name"),
        F.col("item.item_name").alias("item_name"),
        F.col("item.price").alias("price"),
        F.col("item.quantity").alias("quantity"),
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
print(f"Sessões identificadas: {silver.select('session_id').distinct().count()}")

print("\nRejeições por motivo:")
(
    rejeitados
    .groupBy("motivo_rejeicao")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)

# Grava a Silver, os rejeitados e os itens individualizados.
gravar_eventos(silver, "silver", "overwrite")
gravar_eventos(rejeitados, "silver/rejeitados", "overwrite")
gravar_eventos(silver_itens, "silver/itens", "overwrite")

titulo("Silver concluída")

spark.stop()
