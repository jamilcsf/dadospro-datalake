"""Exporta as duas tabelas Gold para site/mapa_calor.js."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from comum import arquivo_do_projeto, caminho, criar_sessao, exigir_camada, titulo

DESTINO = arquivo_do_projeto("site/mapa_calor.js")
PREFIXO = "window.MAPA_CALOR = "
COLUNAS = {
    "mapa_calor_cliques": ["pagina", "element_id", "cliques", "sessoes", "sessoes_na_pagina", "taxa_clique", "intensidade"],
    "mapa_calor_rolagem": ["pagina", "marco", "sessoes_que_chegaram", "pct_sessoes"],
}


def montar_js(cliques: list[dict], rolagem: list[dict], gerado_em: str) -> str:
    dados = {"gerado_em": gerado_em, "cliques": cliques, "rolagem": rolagem}
    return PREFIXO + json.dumps(dados, ensure_ascii=True, default=float, allow_nan=False) + ";\n"


def gravar(texto: str, destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".tmp")
    temporario.write_text(texto, encoding="utf-8")
    os.replace(temporario, destino)


def avisos(cliques, rolagem):
    if not cliques:
        return ["Nenhum clique com element_id encontrado. Confira a landing e a Silver antes de regenerar dados."]
    return []


def linhas(spark, tabela, ordem):
    return [linha.asDict() for linha in spark.read.parquet(caminho(f"gold/{tabela}")).select(*COLUNAS[tabela]).orderBy(*ordem).collect()]


def main():
    spark = criar_sessao("exportar-mapa-calor")
    try:
        for tabela in COLUNAS:
            exigir_camada(spark, f"gold/{tabela}", "python pipeline/07_gold_mapa_calor.py")
        cliques = linhas(spark, "mapa_calor_cliques", ["pagina", "element_id"])
        rolagem = linhas(spark, "mapa_calor_rolagem", ["pagina", "marco"])
    finally:
        spark.stop()
    gerado_em = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    gravar(montar_js(cliques, rolagem, gerado_em), DESTINO)
    titulo("Mapa de calor exportado")
    print(f"{len(cliques)} elementos e {len(rolagem)} marcos de rolagem em {DESTINO}")
    for aviso in avisos(cliques, rolagem):
        print(f"AVISO: {aviso}")
    print('Abra site/index.html e clique em "Mapa de calor" no painel.')


if __name__ == "__main__":
    main()
