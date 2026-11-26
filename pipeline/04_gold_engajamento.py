"""Gold — sessões, retenção de vídeo, buscas FAQ e saídas/downloads."""
from pyspark.sql import functions as F
from comum import criar_sessao, caminho, ler_eventos, exigir_camada, titulo


def calcular_sessoes(silver):
    ordem = F.struct('ts', 'event_id')
    return (
        silver.groupBy('session_id').agg(
            F.min_by('visitante_id', ordem).alias('visitante_id'),
            F.min('ts').alias('inicio'),
            F.max('ts').alias('fim'),
            F.count('*').alias('total_eventos'),
            F.max('percent_scrolled').alias('scroll_maximo'),
            F.max('video_percent').alias('video_percent_maximo'),
            F.max(F.when(F.col('event_name') == 'generate_lead', 1).otherwise(0)).cast('boolean').alias('fez_lead'),
            F.max(F.when(F.col('event_name') == 'purchase', 1).otherwise(0)).cast('boolean').alias('comprou'),
            F.min_by(F.struct('source', 'medium', 'device_category', 'device_browser', 'device_os'), ordem).alias('primeiro_evento'),
        )
        .withColumn('duracao_segundos', (F.unix_micros('fim') - F.unix_micros('inicio')) / 1000000)
        .withColumn('canal', F.concat_ws(' / ', F.coalesce(F.col('primeiro_evento.source'), F.lit('(direct)')), F.coalesce(F.col('primeiro_evento.medium'), F.lit('(none)'))))
        .withColumn('dispositivo', F.col('primeiro_evento.device_category'))
        .withColumn('navegador', F.col('primeiro_evento.device_browser'))
        .withColumn('sistema_operacional', F.col('primeiro_evento.device_os'))
        .drop('primeiro_evento')
    )


def calcular_retencao_video(silver):
    marcos = [10, 25, 50, 75, 100]
    videos = (
        silver.filter(F.col('event_name').isin('video_start', 'video_progress', 'video_complete'))
        .groupBy('session_id', 'video_title').agg(
            F.max(F.when(F.col('event_name') == 'video_start', 1).otherwise(0)).alias('iniciou_video'),
            F.max('video_percent').alias('percentual_maximo'),
        ).filter(F.col('iniciou_video') == 1)
    )
    retencao = videos.groupBy('video_title').agg(
        F.countDistinct('session_id').alias('sessoes_com_video_start'),
        *[F.countDistinct(F.when(F.col('percentual_maximo') >= m, F.col('session_id'))).alias(f'sessoes_{m}') for m in marcos],
    )
    for m in marcos:
        retencao = retencao.withColumn(f'retencao_{m}', F.round(F.col(f'sessoes_{m}') / F.col('sessoes_com_video_start') * 100, 2))
    return retencao


def calcular_buscas_faq(silver):
    return (
        silver.filter(F.col('event_name') == 'site_search')
        .select(F.trim('search_term').alias('termo'), F.col('search_results').alias('resultados'))
        .filter(F.col('termo').isNotNull() & (F.col('termo') != ''))
        .groupBy('termo').agg(
            F.count('*').alias('total_buscas'),
            F.sum(F.when(F.col('resultados') == 0, 1).otherwise(0)).alias('buscas_sem_resultado'),
        )
    )


def calcular_saidas_downloads(silver):
    cliques = (
        silver.filter((F.col('event_name') == 'click') & F.col('outbound') & F.col('link_domain').isNotNull())
        .groupBy(F.col('link_domain').alias('destino')).agg(F.count('*').alias('total_eventos'))
        .withColumn('tipo', F.lit('clique_externo'))
    )
    downloads = (
        silver.filter((F.col('event_name') == 'file_download') & F.col('file_name').isNotNull())
        .groupBy(F.col('file_name').alias('destino')).agg(F.count('*').alias('total_eventos'))
        .withColumn('tipo', F.lit('download'))
    )
    return cliques.unionByName(downloads).select('tipo', 'destino', 'total_eventos')


def main():
    spark = criar_sessao('gold_engajamento')
    silver = None
    try:
        exigir_camada(spark, 'silver', 'python pipeline/02_silver_eventos.py')
        silver = ler_eventos(spark, 'silver', 'parquet').cache()
        tabelas = [
            ('sessoes', 'Sessões', calcular_sessoes),
            ('retencao_video', 'Retenção de vídeo', calcular_retencao_video),
            ('buscas_faq', 'Buscas FAQ', calcular_buscas_faq),
            ('saidas_downloads', 'Saídas e downloads', calcular_saidas_downloads),
        ]
        for nome, rotulo, calcular in tabelas:
            destino = caminho(f'gold/{nome}')
            calcular(silver).write.mode('overwrite').parquet(destino)
            df = spark.read.parquet(destino)
            titulo(f'{rotulo} - Gold')
            if nome == 'sessoes':
                print(f'Total de sessões: {df.count()}')
                df.orderBy('session_id').show(5, truncate=False)
            elif nome == 'buscas_faq':
                print('Total de pesquisas:', df.agg(F.sum('total_buscas')).first()[0] or 0)
                df.orderBy(F.desc('total_buscas'), 'termo').show(truncate=False)
            elif nome == 'saidas_downloads':
                df.orderBy('tipo', F.desc('total_eventos'), 'destino').show(truncate=False)
            else:
                df.orderBy('video_title').show(truncate=False)
            print(f'Tabela gold/{nome} gravada com sucesso.')
        print('\nJob gold_engajamento concluido com sucesso.')
    finally:
        if silver is not None:
            silver.unpersist()
        spark.stop()


if __name__ == '__main__':
    main()
