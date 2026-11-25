"""Exporta os mapas de calor da Gold para o site (site/mapa_calor.js).

Executar na raiz do projeto:
    docker compose run --rm app python pipeline/exportar_mapa_calor.py

As tabelas pequenas são lidas pelo Spark diretamente do S3/Floci e
convertidas em um arquivo JavaScript consumido pelo site.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from comum import (
    arquivo_do_projeto,
    caminho,
    criar_sessao,
    exigir_camada,
    titulo,
)


DESTINO = arquivo_do_projeto("site/mapa_calor.js")
PREFIXO = "window.MAPA_CALOR = "


def montar_js(cliques: list[dict], rolagem: list[dict], gerado_em: str) -> str:
    """Monta o objeto JavaScript com os dois mapas e a data de geração."""
    dados = {
        "gerado_em": gerado_em,
        "cliques": cliques,
        "rolagem": rolagem,
    }
    return PREFIXO + json.dumps(
        dados,
        ensure_ascii=False,
        default=float,
    ) + ";\n"


def gravar(texto: str, destino: Path) -> None:
    """Grava em temporário e renomeia para evitar arquivo parcial."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".tmp")
    temporario.write_text(texto, encoding="utf-8")
    os.replace(temporario, destino)


def avisos(cliques: list[dict], rolagem: list[dict]) -> list[str]:
    """Explica quando a tabela de cliques não contém elementos."""
    if cliques:
        return []

    return [
        "Nenhum clique com element_id foi encontrado na Silver: a landing "
        "provavelmente foi gerada sem element_id. Rode novamente "
        "gerador/gerar_eventos.py e execute o pipeline inteiro."
    ]


def linhas(spark, tabela: str, ordem: list[str]) -> list[dict]:
    """Lê uma tabela Gold pequena e retorna suas linhas como dicionários."""
    return [
        linha.asDict()
        for linha in (
            spark.read.parquet(caminho(f"gold/{tabela}"))
            .orderBy(*ordem)
            .collect()
        )
    ]


def main() -> None:
    spark = criar_sessao("exportar-mapa-calor")

    try:
        for tabela in ("mapa_calor_cliques", "mapa_calor_rolagem"):
            exigir_camada(
                spark,
                f"gold/{tabela}",
                "python pipeline/mapa_calor_cliques.py e "
                "python pipeline/mapa_calor_rolagem.py",
            )

        cliques = linhas(
            spark,
            "mapa_calor_cliques",
            ["pagina", "element_id"],
        )
        rolagem = linhas(
            spark,
            "mapa_calor_rolagem",
            ["pagina", "marco"],
        )
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
