"""Gold — funis de compra e de geração de leads.

Produz gold/funil_compra e gold/funil_lead.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from comum import criar_sessao, caminho, ler_eventos, exigir_camada, titulo

ETAPAS_COMPRA = [
    'view_item_list', 'select_item', 'add_to_cart', 'view_cart',
    'begin_checkout', 'add_payment_info', 'purchase',
]
ETAPAS_LEAD = ['page_view', 'scroll_50', 'generate_lead']


def identificar_canais(silver):
    janela = Window.partitionBy('session_id').orderBy('ts', 'event_id')
    return (
        silver.withColumn('_ordem', F.row_number().over(janela))
        .filter(F.col('_ordem') == 1)
        .select('session_id', F.concat_ws(
            ' / ', F.coalesce(F.col('source'), F.lit('(direct)')),
            F.coalesce(F.col('medium'), F.lit('(none)')),
        ).alias('canal'))
    )


def identificar_planos(itens):
    janela = Window.partitionBy('session_id').orderBy('ts', 'event_id', 'posicao')
    return (
        itens.filter(F.col('event_name') == 'select_item')
        .withColumn('_ordem', F.row_number().over(janela))
        .filter(F.col('_ordem') == 1)
        .select('session_id', F.col('item_id').alias('plano'))
    )


def agregar_indicadores(sessoes, dimensoes, indicadores):
    agregacoes = [F.coalesce(F.sum(c), F.lit(0)).alias(c) for c in indicadores]
    por_dimensao = sessoes.groupBy(*dimensoes).agg(*agregacoes)
    total = sessoes.agg(*agregacoes)
    for dimensao in dimensoes:
        total = total.withColumn(dimensao, F.lit('(todos)'))
    return por_dimensao.unionByName(total.select(*dimensoes, *indicadores))


def montar_funil(resumo, dimensoes, etapas):
    linhas = [F.struct(F.lit(etapa).alias('etapa'), F.lit(ordem).alias('ordem'),
                       F.col(etapa).alias('sessoes'))
              for ordem, etapa in enumerate(etapas, start=1)]
    funil = resumo.select(*dimensoes, F.explode(F.array(*linhas)).alias('item')).select(*dimensoes, 'item.*')
    janela = Window.partitionBy(*dimensoes).orderBy('ordem')
    completa = janela.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    return (
        funil.withColumn('_anterior', F.lag('sessoes').over(janela))
        .withColumn('_topo', F.first('sessoes').over(completa))
        .withColumn('conversao_etapa_anterior', F.when(F.col('_anterior') > 0, F.round(F.col('sessoes') / F.col('_anterior'), 4)))
        .withColumn('conversao_topo', F.when(F.col('_topo') > 0, F.round(F.col('sessoes') / F.col('_topo'), 4)))
        .select(*dimensoes, 'etapa', 'ordem', 'sessoes', 'conversao_etapa_anterior', 'conversao_topo')
    )


def calcular_funil_compra(silver, itens, canais):
    indicadores = ETAPAS_COMPRA + ['view_item']
    sessoes = (
        silver.groupBy('session_id').agg(*[
            F.max(F.when(F.col('event_name') == evento, 1).otherwise(0)).alias(evento)
            for evento in indicadores
        ]).join(canais, 'session_id', 'left')
        .join(identificar_planos(itens), 'session_id', 'left')
        .withColumn('plano', F.coalesce(F.col('plano'), F.lit('(nenhum)')))
    )
    resumo = agregar_indicadores(sessoes, ['plano', 'canal'], indicadores)
    funil = montar_funil(resumo, ['plano', 'canal'], ETAPAS_COMPRA)
    complemento = resumo.select(
        'plano', 'canal', F.col('view_item').alias('sessoes_view_item'),
        F.when(F.col('select_item') > 0, F.round(F.col('view_item') / F.col('select_item'), 4)).alias('taxa_view_item'),
    )
    return funil.join(complemento, ['plano', 'canal'], 'left')


def pagina_do_evento():
    url = F.trim(F.col('page_location'))
    pagina = F.regexp_replace(url, r'(?i)^https?://[^/?#]+', '')
    pagina = F.regexp_replace(pagina, r'[?#].*$', '')
    return F.when(url.isNotNull() & (url != ''), F.when(pagina == '', F.lit('/')).otherwise(pagina))


def calcular_funil_lead(silver, canais):
    landing = silver.filter(pagina_do_evento() == '/')
    sessoes = (
        landing.groupBy('session_id').agg(
            F.max(F.when(F.col('event_name') == 'page_view', 1).otherwise(0)).alias('page_view'),
            F.max(F.when((F.col('event_name') == 'scroll') & (F.col('percent_scrolled') >= 50), 1).otherwise(0)).alias('scroll_50'),
            F.max(F.when(F.col('event_name') == 'generate_lead', 1).otherwise(0)).alias('generate_lead'),
        ).join(canais, 'session_id', 'left')
    )
    resumo = agregar_indicadores(sessoes, ['canal'], ETAPAS_LEAD)
    return montar_funil(resumo, ['canal'], ETAPAS_LEAD)


def main():
    spark = criar_sessao('gold_funil')
    persistidos = []
    try:
        exigir_camada(spark, 'silver', 'python pipeline/02_silver_eventos.py')
        exigir_camada(spark, 'silver/itens', 'python pipeline/02_silver_eventos.py')
        silver = ler_eventos(spark, 'silver', 'parquet').cache()
        persistidos.append(silver)
        itens = spark.read.parquet(caminho('silver/itens'))
        canais = identificar_canais(silver).cache()
        persistidos.append(canais)
        funil_compra = calcular_funil_compra(silver, itens, canais)
        funil_compra.write.mode('overwrite').parquet(caminho('gold/funil_compra'))
        titulo('Funil de compra - Gold')
        spark.read.parquet(caminho('gold/funil_compra')).filter(
            (F.col('plano') == '(todos)') & (F.col('canal') == '(todos)')
        ).orderBy('ordem').show(truncate=False)
        print('Tabela gold/funil_compra gravada com sucesso.')
        funil_lead = calcular_funil_lead(silver, canais)
        funil_lead.write.mode('overwrite').parquet(caminho('gold/funil_lead'))
        titulo('Funil de leads - Gold')
        spark.read.parquet(caminho('gold/funil_lead')).filter(
            F.col('canal') == '(todos)'
        ).orderBy('ordem').show(truncate=False)
        print('Tabela gold/funil_lead gravada com sucesso.')
        print('\nJob gold_funil concluido com sucesso.')
    finally:
        for df in reversed(persistidos):
            df.unpersist()
        spark.stop()


if __name__ == '__main__':
    main()
