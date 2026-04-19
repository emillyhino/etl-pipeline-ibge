## O que o pipeline faz

**Extract:** coleta dados de duas APIs do IBGE — localidades e população por município (Censo 2021)

**Transform:** merge dos datasets, limpeza de nulos, padronização de campos, remoção de duplicatas e enriquecimento com região e mesorregião

**Load:** carga no banco SQLite com criação de índices para performance e log completo de execução

## Resultados

- 5.570 municípios carregados
- 27 estados cobertos
- São Paulo lidera com 12,3 milhões de habitantes
- Minas Gerais tem o maior número de municípios (853)

## Tecnologias

- Python 3 · Requests · Pandas · SQLite · Loguru

## Como executar

```bash
git clone https://github.com/emillyhino/etl-pipeline-ibge.git
cd etl-pipeline-ibge
pip install requests pandas loguru
python etl.py
```

## Fonte dos dados

IBGE — API de Localidades e Agregados  
https://servicodados.ibge.gov.br

## Autora

**Emilly Hino**  
Bacharela em Ciência de Dados
[LinkedIn](https://linkedin.com/in/emillyhino) · [GitHub](https://github.com/emillyhino)