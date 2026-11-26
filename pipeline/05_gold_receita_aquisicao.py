"""Gold: receita, reembolsos, order bump e conversão de leads."""
from pyspark.sql import Window, functions as F
from comum import criar_sessao, caminho, ler_eventos, exigir_camada, titulo

TODOS = '(todos)'


def canais_sessao(eventos):
    ordem = Window.partitionBy('session_id').orderBy('ts', 'event_id')
    return (eventos.withColumn('_ordem', F.row_number().over(ordem))
            .filter(F.col('_ordem') == 1)
            .select('session_id', F.concat_ws(' / ',
                F.coalesce('source', F.lit('(direct)')),
                F.coalesce('medium', F.lit('(none)'))).alias('canal')))


def compras_contexto(eventos, itens, canais):
    ordem = Window.partitionBy('transaction_id').orderBy('ts', 'event_id')
    compras = (eventos.filter(F.col('event_name') == 'purchase')
               .withColumn('_ordem', F.row_number().over(ordem))
               .filter(F.col('_ordem') == 1)
               .select('transaction_id', 'event_id', 'session_id',
                       F.to_date('ts').alias('data'),
                       F.col('value').cast('decimal(24,2)').alias('receita_bruta')))
    itens_compra = itens.filter(F.col('event_name') == 'purchase')
    planos = (itens_compra.filter(F.col('item_category') == 'plano')
              .groupBy('event_id').agg(F.min_by('item_id', F.struct('posicao', 'item_id')).alias('plano')))
    bumps = (itens_compra.groupBy('event_id').agg(F.max(F.when(
        F.col('item_id') == 'BUMP-MENTORIA', 1).otherwise(0)).alias('comprou_order_bump')))
    reembolsos = (eventos.filter(F.col('event_name') == 'refund')
                  .groupBy('transaction_id').agg(
                      F.sum(F.col('value').cast('decimal(24,2)')).alias('receita_reembolsada'),
                      F.lit(1).alias('teve_reembolso')))
    return (compras.join(planos, 'event_id', 'left').join(bumps, 'event_id', 'left')
            .join(reembolsos, 'transaction_id', 'left').join(canais, 'session_id', 'left')
            .fillna({'plano':'(nenhum)', 'canal':'(desconhecido)', 'teve_reembolso':0,
                     'comprou_order_bump':0})
            .withColumn('receita_reembolsada', F.coalesce('receita_reembolsada', F.lit(0).cast('decimal(24,2)')))
            .withColumn('receita_liquida', F.col('receita_bruta') - F.col('receita_reembolsada')))


def calcular_receita(compras):
    return compras.groupBy('data','plano','canal').agg(
        F.sum('receita_bruta').alias('receita_bruta'),
        F.sum('receita_reembolsada').alias('receita_reembolsada'),
        F.sum('receita_liquida').alias('receita_liquida'),
        F.count('*').alias('pedidos'),
        F.round(F.avg('receita_bruta'),2).alias('ticket_medio'))


def calcular_reembolso(compras):
    return (compras.groupBy('plano','canal').agg(F.count('*').alias('pedidos'),
        F.sum('teve_reembolso').alias('pedidos_reembolsados'))
        .withColumn('taxa_reembolso', F.round(F.try_divide('pedidos_reembolsados','pedidos'),4)))


def calcular_bump(compras):
    def agregar(base):
        return base.groupBy('plano').agg(F.count('*').alias('total_compras'),
            F.coalesce(F.sum('comprou_order_bump'), F.lit(0)).alias('compras_com_order_bump'))
    return (agregar(compras).unionByName(agregar(compras.withColumn('plano', F.lit(TODOS))))
            .withColumn('adesao_percentual', F.round(F.try_divide('compras_com_order_bump','total_compras'),4)))


def calcular_leads(eventos, canais):
    ordem = Window.partitionBy('visitante_id').orderBy('ts','event_id')
    leads = (eventos.filter((F.col('event_name') == 'generate_lead') & F.col('visitante_id').isNotNull())
             .withColumn('_ordem', F.row_number().over(ordem)).filter(F.col('_ordem') == 1)
             .join(canais, 'session_id','left').select(F.col('visitante_id').alias('pessoa_id'),
                 F.col('ts').alias('data_lead'),'canal').fillna({'canal':'(desconhecido)'}))
    posteriores = (eventos.filter(F.col('event_name') == 'purchase')
                    .select(F.col('visitante_id').alias('pessoa_id'), F.col('ts').alias('data_compra'))
                    .join(leads.select('pessoa_id','data_lead'),'pessoa_id')
                    .filter(F.col('data_compra') > F.col('data_lead'))
                    .groupBy('pessoa_id').agg(F.min('data_compra').alias('data_compra')))
    base = (leads.join(posteriores,'pessoa_id','left').withColumn('dias',
        (F.unix_micros('data_compra') - F.unix_micros('data_lead')) / 86400000000.0))
    def agregar(df):
        return df.groupBy('canal').agg(F.count('*').alias('leads'),
            F.count('data_compra').alias('compraram_depois'),
            F.round(F.percentile_approx('dias',0.5),2).alias('mediana_dias'))
    return (agregar(base).unionByName(agregar(base.withColumn('canal',F.lit(TODOS))))
            .withColumn('taxa_conversao', F.round(F.try_divide('compraram_depois','leads'),4)))


def gravar(spark, df, nome):
    destino = caminho('gold/' + nome)
    df.write.mode('overwrite').parquet(destino)
    return spark.read.parquet(destino)


def main():
    spark = criar_sessao('gold_receita_aquisicao')
    persistidos = []
    try:
        exigir_camada(spark,'silver','python pipeline/02_silver_eventos.py')
        exigir_camada(spark,'silver/itens','python pipeline/02_silver_eventos.py')
        eventos = ler_eventos(spark,'silver','parquet').cache()
        persistidos.append(eventos)
        itens = spark.read.parquet(caminho('silver/itens'))
        canais = canais_sessao(eventos).cache()
        persistidos.append(canais)
        compras = compras_contexto(eventos,itens,canais).cache()
        persistidos.append(compras)
        if compras.filter(F.col('receita_bruta').isNull()).count():
            raise ValueError('Compra sem valor monetário válido; conferir Silver.')
        titulo('Receita diária - Gold')
        receita = gravar(spark,calcular_receita(compras),'receita_diaria')
        receita.orderBy('data','plano','canal').show(10,truncate=False)
        receita.agg(F.sum('receita_bruta').alias('receita_bruta'),
                    F.sum('receita_reembolsada').alias('receita_reembolsada'),
                    F.sum('receita_liquida').alias('receita_liquida'),
                    F.sum('pedidos').alias('pedidos')).withColumn('ticket_medio',
                    F.round(F.try_divide('receita_bruta','pedidos'),2)).show(truncate=False)
        print('Tabela gold/receita_diaria gravada com sucesso.')
        titulo('Taxa de reembolso - Gold')
        taxa = gravar(spark,calcular_reembolso(compras),'taxa_reembolso')
        taxa.orderBy('plano','canal').show(100,truncate=False)
        taxa.groupBy('canal').agg(F.sum('pedidos').alias('pedidos'),
            F.sum('pedidos_reembolsados').alias('pedidos_reembolsados')).withColumn('taxa_reembolso',
            F.round(F.try_divide('pedidos_reembolsados','pedidos'),4)).orderBy('canal').show(truncate=False)
        print('Tabela gold/taxa_reembolso gravada com sucesso.')
        titulo('Adesão ao order bump - Gold')
        gravar(spark,calcular_bump(compras),'adesao_order_bump').orderBy('plano').show(truncate=False)
        print('Tabela gold/adesao_order_bump gravada com sucesso.')
        titulo('Conversão de leads em compras - Gold')
        gravar(spark,calcular_leads(eventos,canais),'lead_para_compra').orderBy('canal').show(truncate=False)
        print('Tabela gold/lead_para_compra gravada com sucesso.')
        print('Job gold_receita_aquisicao concluido com sucesso.')
    finally:
        for df in reversed(persistidos):
            df.unpersist()
        spark.stop()


if __name__ == '__main__':
    main()
