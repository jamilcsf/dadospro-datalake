
"""Camada Silver: qualidade, tipagem, sessões e identidade."""

from pyspark.sql import DataFrame
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


# Catálogo de eventos definido no contrato.
EVENTOS_VALIDOS = [
    "page_view", "scroll", "click", "site_search",
    "video_start", "video_progress", "video_complete",
    "file_download", "view_item_list", "select_item",
    "view_item", "add_to_cart", "remove_from_cart",
    "view_cart", "begin_checkout", "add_shipping_info",
    "add_payment_info", "purchase", "refund",
    "generate_lead", "sign_up", "login",
]

MARCOS_SCROLL = [25, 50, 75, 90]
MARCOS_VIDEO = [0, 10, 25, 50, 75, 100]

FORMATO_TS = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX"

PARTICOES_ITENS = ["year", "month", "day"]

PARAMETROS_TEXTO = [
    "coupon",
    "currency",
    "element_id",
    "file_extension",
    "file_name",
    "item_list_id",
    "item_list_name",
    "lead_source",
    "link_domain",
    "link_id",
    "link_text",
    "link_url",
    "method",
    "page_title",
    "payment_type",
    "reason",
    "search_area",
    "search_term",
    "shipping_tier",
    "transaction_id",
    "video_provider",
    "video_title",
]

PARAMETROS_INTEIROS = [
    "percent_scrolled",
    "search_results",
    "video_current_time",
    "video_duration",
    "video_percent",
]

PARAMETROS_DECIMAIS = [
    "shipping",
    "tax",
    "value",
]


def id_ausente():
    """Identifica IDs nulos ou vazios."""

    return (
        F.col("event_id").isNull()
        | (F.trim(F.col("event_id")) == "")
    )


def deduplicar(eventos: DataFrame) -> DataFrame:
    """R5: mantém a primeira ingestão de cada event_id válido."""

    janela = (
        Window
        .partitionBy("event_id")
        .orderBy(
            F.col("instante_ingestao").asc_nulls_last(),
            F.col("arquivo_origem").asc_nulls_last(),
        )
    )

    identificados = (
        eventos
        .filter(~id_ausente())
        .withColumn(
            "_ordem_deduplicacao",
            F.row_number().over(janela),
        )
        .filter(F.col("_ordem_deduplicacao") == 1)
        .drop("_ordem_deduplicacao")
    )

    # Não agrupa todos os IDs ausentes como uma única duplicata.
    sem_identificacao = eventos.filter(id_ausente())

    return identificados.unionByName(sem_identificacao)


def tipar(eventos: DataFrame) -> DataFrame:
    """R6: converte timestamps e parâmetros em colunas tipadas."""

    eventos = (
        eventos
        .withColumn("event_ts_original", F.col("event_ts"))
        .withColumn(
            "ts",
            F.try_to_timestamp(
                F.col("event_ts"),
                F.lit(FORMATO_TS),
            ),
        )
        .withColumn("event_ts", F.col("ts"))
    )

    return eventos.select(
        "*",

        # Informações da página.
        F.col("page.location").alias("page_location"),
        F.col("page.referrer").alias("page_referrer"),

        # Informações de aquisição.
        F.col("traffic.source").alias("source"),
        F.col("traffic.medium").alias("medium"),
        F.col("traffic.campaign").alias("campaign"),

        # Informações do dispositivo.
        F.col("device.category").alias("device_category"),
        F.col("device.os").alias("device_os"),
        F.col("device.browser").alias("device_browser"),

        # Parâmetros textuais.
        *[
            F.col("params").getItem(nome).alias(nome)
            for nome in PARAMETROS_TEXTO
        ],

        # Parâmetros inteiros.
        *[
            F.expr(
                f"try_cast(params['{nome}'] as int)"
            ).alias(nome)
            for nome in PARAMETROS_INTEIROS
        ],

        # Parâmetros monetários.
        *[
            F.expr(
                f"try_cast(params['{nome}'] as double)"
            ).alias(nome)
            for nome in PARAMETROS_DECIMAIS
        ],

        # Parâmetro booleano.
        F.expr(
            "try_cast(params['outbound'] as boolean)"
        ).alias("outbound"),
    )


def validar(eventos: DataFrame):
    """R7: separa eventos aceitos e rejeitados."""

    valor_original = F.col("params").getItem("value")

    scroll_original = F.col("params").getItem(
        "percent_scrolled"
    )

    video_original = F.col("params").getItem(
        "video_percent"
    )

    valor_invalido = (
        valor_original.isNotNull()
        & (
            F.col("value").isNull()
            | F.isnan("value")
            | (F.abs(F.col("value")) == float("inf"))
            | (F.col("value") < 0)
            | F.col("currency").isNull()
            | (F.col("currency") != "BRL")
        )
    )

    scroll_invalido = (
        scroll_original.isNotNull()
        & (
            F.col("percent_scrolled").isNull()
            | (~F.col("percent_scrolled").isin(MARCOS_SCROLL))
        )
    )

    video_invalido = (
        video_original.isNotNull()
        & (
            F.col("video_percent").isNull()
            | (~F.col("video_percent").isin(MARCOS_VIDEO))
        )
    )

    classificados = eventos.withColumn(
        "motivo_rejeicao",

        # Regra 1: evento fora do catálogo.
        F.when(
            F.col("event_name").isNull()
            | (~F.col("event_name").isin(EVENTOS_VALIDOS)),
            "evento fora do catalogo",
        )

        # Regra 2: timestamp ausente, inválido ou futuro.
        .when(
            F.col("ts").isNull()
            | (F.col("ts") > F.col("instante_ingestao")),
            "timestamp ausente ou futuro",
        )

        # Regra 3: compra ou reembolso sem identificação.
        .when(
            F.col("event_name").isin("purchase", "refund")
            & (
                F.col("transaction_id").isNull()
                | (F.trim(F.col("transaction_id")) == "")
            ),
            "transacao sem id",
        )

        # Regra 4: valor monetário inválido.
        .when(
            valor_invalido,
            "valor monetario invalido",
        )

        # Regra 5: marco de scroll ou vídeo inválido.
        .when(
            scroll_invalido | video_invalido,
            "marco fora do padrao",
        )

        # Regra adicional: evento sem identificador.
        .when(
            id_ausente(),
            "event_id ausente",
        )

        .otherwise(None),
    )

    # Regra 6: reembolso sem compra válida correspondente.
    compras_validas = (
        classificados
        .filter(
            (F.col("event_name") == "purchase")
            & F.col("motivo_rejeicao").isNull()
        )
        .select("transaction_id")
        .distinct()
        .withColumn("_compra_encontrada", F.lit(True))
    )

    classificados = (
        classificados
        .join(
            compras_validas,
            on="transaction_id",
            how="left",
        )
        .withColumn(
            "motivo_rejeicao",
            F.when(
                (F.col("event_name") == "refund")
                & F.col("motivo_rejeicao").isNull()
                & F.col("_compra_encontrada").isNull(),
                "reembolso sem compra",
            ).otherwise(F.col("motivo_rejeicao")),
        )
        .drop("_compra_encontrada")
    )

    aceitos = (
        classificados
        .filter(F.col("motivo_rejeicao").isNull())
        .drop("motivo_rejeicao")
    )

    rejeitados = classificados.filter(
        F.col("motivo_rejeicao").isNotNull()
    )

    return aceitos, rejeitados


def sessionizar(eventos: DataFrame) -> DataFrame:
    """R8: cria sessões por inatividade superior a 30 minutos."""

    eventos = eventos.withColumn(
        "_chave_sessao",
        F.coalesce(
            F.col("user_pseudo_id"),
            F.concat(F.lit("evento_"), F.col("event_id")),
        ),
    )

    janela = (
        Window
        .partitionBy("_chave_sessao")
        .orderBy("ts", "event_id")
    )

    janela_acumulada = janela.rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )

    return (
        eventos
        .withColumn(
            "_evento_anterior",
            F.lag("ts").over(janela),
        )
        .withColumn(
            "_intervalo_segundos",
            (
                F.unix_micros("ts")
                - F.unix_micros("_evento_anterior")
            ) / F.lit(1_000_000),
        )
        .withColumn(
            "_nova_sessao",
            F.when(
                F.col("_evento_anterior").isNull()
                | (F.col("_intervalo_segundos") > 1800),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "numero_sessao",
            F.sum("_nova_sessao").over(janela_acumulada),
        )
        .withColumn(
            "session_id",
            F.concat_ws(
                "-",
                F.col("_chave_sessao"),
                F.col("numero_sessao").cast("string"),
            ),
        )
        .drop(
            "_chave_sessao",
            "_evento_anterior",
            "_intervalo_segundos",
            "_nova_sessao",
        )
    )


def resolver_identidade(eventos: DataFrame) -> DataFrame:
    """R9: propaga retroativamente o user_id conhecido."""

    janela = (
        Window
        .partitionBy("user_pseudo_id")
        .orderBy(
            F.col("ts").desc(),
            F.col("event_id").desc(),
        )
    )

    identidades = (
        eventos
        .filter(
            F.col("user_pseudo_id").isNotNull()
            & F.col("user_id").isNotNull()
        )
        .withColumn(
            "_ordem_identidade",
            F.row_number().over(janela),
        )
        .filter(F.col("_ordem_identidade") == 1)
        .select(
            "user_pseudo_id",
            F.col("user_id").alias("user_id_resolvido"),
        )
    )

    return (
        eventos
        .join(
            identidades,
            on="user_pseudo_id",
            how="left",
        )
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


def explodir_itens(eventos: DataFrame) -> DataFrame:
    """R10: cria um registro por item associado ao evento."""

    return (
        eventos
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
            F.col("item.quantity").cast("long").alias("quantity"),
        )
    )


def main() -> None:
    """Executa o job completo da camada Silver."""

    spark = criar_sessao("silver")
    caches = []

    try:
        exigir_camada(
            spark,
            glob_eventos("bronze"),
            "docker compose run --rm app python pipeline/01_bronze_ingestao.py",
        )

        titulo("Validação e transformação - Silver")

        bronze = ler_eventos(spark, "bronze").cache()
        caches.append(bronze)

        total = bronze.count()

        ids_nulos = bronze.filter(id_ausente()).count()

        # R5: deduplicação.
        deduplicados = deduplicar(bronze).cache()
        caches.append(deduplicados)

        total_deduplicados = deduplicados.count()
        duplicados = total - total_deduplicados

        # R6: tipagem.
        tipados = tipar(deduplicados)

        # R7: validação.
        aceitos, rejeitados = validar(tipados)

        rejeitados = rejeitados.cache()
        caches.append(rejeitados)

        # R8 e R9: sessões e identidade.
        silver = resolver_identidade(
            sessionizar(aceitos)
        ).cache()

        caches.append(silver)

        # R10: tabela de itens.
        silver_itens = explodir_itens(silver)

        # Diagnósticos.
        total_aceitos = silver.count()
        total_rejeitados = rejeitados.count()
        total_itens = silver_itens.count()

        total_sessoes = (
            silver
            .select("session_id")
            .distinct()
            .count()
        )

        print(f"Registros na Bronze: {total}")
        print(f"IDs ausentes: {ids_nulos}")
        print(f"Duplicatas excedentes: {duplicados}")
        print(f"Registros após deduplicação: {total_deduplicados}")
        print(f"Eventos aceitos: {total_aceitos}")
        print(f"Eventos rejeitados: {total_rejeitados}")
        print(f"Sessões identificadas: {total_sessoes}")
        print(f"Itens extraídos: {total_itens}")

        # Verifica a reconciliação das contagens.
        if total != (
            duplicados + total_aceitos + total_rejeitados
        ):
            raise ValueError(
                "As contagens da Silver não reconciliam com a Bronze."
            )

        print("\nReconciliação das contagens: OK")

        print("\nRejeições por motivo:")

        (
            rejeitados
            .groupBy("motivo_rejeicao")
            .count()
            .orderBy(F.desc("count"))
            .show(truncate=False)
        )

        # Eventos: event_name/year/month/day.
        gravar_eventos(
            silver,
            "silver",
            "overwrite",
        )

        # Itens: somente year/month/day.
        (
            silver_itens
            .write
            .mode("overwrite")
            .option("partitionOverwriteMode", "static")
            .partitionBy(*PARTICOES_ITENS)
            .parquet(caminho("silver/itens"))
        )

        # Rejeitados: preserva o timestamp original.
        (
            rejeitados
            .withColumn("event_ts", F.col("event_ts_original"))
            .write
            .mode("overwrite")
            .parquet(caminho("silver/eventos_rejeitados"))
        )

        titulo("Silver concluída")

    finally:
        for dataframe in reversed(caches):
            dataframe.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
