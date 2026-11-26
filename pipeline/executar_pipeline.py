"""Executa os oito jobs do datalake DadosPro, sem gerar dados."""
from pathlib import Path
import os
import subprocess
import sys
from time import perf_counter

PASTA = Path(__file__).resolve().parent
RAIZ = PASTA.parent
JOBS = (
    '01_bronze_ingestao.py',
    '02_silver_eventos.py',
    '03_gold_funil.py',
    '04_gold_engajamento.py',
    '05_gold_receita_aquisicao.py',
    '06_gold_ml.py',
    '07_gold_mapa_calor.py',
    '08_exportar_mapa_calor.py',
)


def main() -> int:
    ausentes = [job for job in JOBS if not (PASTA / job).is_file()]
    if ausentes:
        print('Jobs ausentes: ' + ', '.join(ausentes), file=sys.stderr, flush=True)
        return 1

    inicio = perf_counter()
    tempos = []
    print('DadosPro — execução sequencial do pipeline', flush=True)
    print('Bucket: ' + os.environ.get('AWS_S3_BUCKET', 'datalake-tracking'), flush=True)
    for numero, job in enumerate(JOBS, 1):
        print(f'\n[{numero}/{len(JOBS)}] Executando {job}', flush=True)
        etapa = perf_counter()
        resultado = subprocess.run([sys.executable, '-u', str(PASTA / job)], cwd=RAIZ)
        duracao = perf_counter() - etapa
        if resultado.returncode != 0:
            print(f'FALHA: {job} (código {resultado.returncode}, {duracao:.2f}s). Pipeline interrompido.', file=sys.stderr, flush=True)
            return resultado.returncode if resultado.returncode > 0 else 1
        tempos.append((job, duracao))
        print(f'OK: {job} ({duracao:.2f}s)', flush=True)

    print('\nRESUMO DA EXECUÇÃO', flush=True)
    for job, duracao in tempos:
        print(f'{job:30s} {duracao:10.2f}s', flush=True)
    print(f'Tempo total: {perf_counter() - inicio:.2f}s', flush=True)
    print('Pipeline concluído com sucesso: 8/8 jobs.', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
