import requests
import pandas as pd
import sqlite3
from datetime import datetime
from loguru import logger

# ── CONFIGURAÇÃO ──────────────────────────────────────────────────
DB_PATH = "ibge_populacao.db"
LOG_PATH = "etl.log"
logger.add(LOG_PATH, rotation="1 MB")

# ── EXTRACT ───────────────────────────────────────────────────────
def extract():
    logger.info("Iniciando extração de dados da API do IBGE...")

    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    municipios = response.json()
    logger.info(f"Municípios extraídos: {len(municipios)}")

    url_pop = "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2021/variaveis/9324?localidades=N6[all]"
    response_pop = requests.get(url_pop, timeout=30)
    response_pop.raise_for_status()
    pop_data = response_pop.json()
    logger.info("Dados populacionais extraídos com sucesso")

    return municipios, pop_data


# ── TRANSFORM ─────────────────────────────────────────────────────
def transform(municipios, pop_data):
    logger.info("Iniciando transformação dos dados...")

    # Municípios
    df_mun = pd.DataFrame([{
        "id_municipio": m["id"],
        "nome":         m["nome"],
        "id_uf":        m["microrregiao"]["mesorregiao"]["UF"]["id"] if m.get("microrregiao") else None,
        "uf_sigla":     m["microrregiao"]["mesorregiao"]["UF"]["sigla"] if m.get("microrregiao") else None,
        "uf_nome":      m["microrregiao"]["mesorregiao"]["UF"]["nome"] if m.get("microrregiao") else None,
        "regiao":       m["microrregiao"]["mesorregiao"]["UF"]["regiao"]["nome"] if m.get("microrregiao") else None,
        "mesorregiao":  m["microrregiao"]["mesorregiao"]["nome"] if m.get("microrregiao") else None,
        "microrregiao": m["microrregiao"]["nome"] if m.get("microrregiao") else None,
    } for m in municipios])

    # População
    registros_pop = []
    for variavel in pop_data:
        for resultado in variavel.get("resultados", []):
            for loc in resultado.get("series", []):
                id_mun = int(loc["localidade"]["id"])
                for ano, valor in loc["serie"].items():
                    try:
                        registros_pop.append({
                            "id_municipio": id_mun,
                            "ano":          int(ano),
                            "populacao":    int(valor)
                        })
                    except:
                        pass

    df_pop = pd.DataFrame(registros_pop)

    # Merge
    df = df_mun.merge(df_pop, on="id_municipio", how="left")
    df = df.dropna(subset=["populacao"])
    df["populacao"] = df["populacao"].astype(int)
    df["data_carga"] = datetime.now().isoformat()

    # Limpeza
    df["nome"] = df["nome"].str.strip()
    df["uf_sigla"] = df["uf_sigla"].str.upper()
    df = df.drop_duplicates(subset=["id_municipio", "ano"])

    logger.info(f"Registros após transformação: {len(df)}")
    logger.info(f"Estados únicos: {df['uf_sigla'].nunique()}")
    logger.info(f"Municípios únicos: {df['id_municipio'].nunique()}")

    return df


# ── LOAD ──────────────────────────────────────────────────────────
def load(df):
    logger.info(f"Carregando dados no banco {DB_PATH}...")

    conn = sqlite3.connect(DB_PATH)

    df.to_sql("municipios_populacao", conn, if_exists="replace", index=False)

    # Criar índices para performance
    conn.execute("CREATE INDEX IF NOT EXISTS idx_uf ON municipios_populacao(uf_sigla)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_regiao ON municipios_populacao(regiao)")
    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM municipios_populacao").fetchone()[0]
    conn.close()

    logger.info(f"Carga concluída! {total} registros no banco.")


# ── ANÁLISES PÓS-CARGA ────────────────────────────────────────────
def analisar():
    logger.info("Executando análises pós-carga...")
    conn = sqlite3.connect(DB_PATH)

    print("\n📊 POPULAÇÃO POR REGIÃO")
    df_reg = pd.read_sql("""
        SELECT regiao,
               COUNT(*) AS municipios,
               SUM(populacao) AS populacao_total,
               ROUND(AVG(populacao), 0) AS media_por_municipio
        FROM municipios_populacao
        GROUP BY regiao
        ORDER BY populacao_total DESC
    """, conn)
    print(df_reg.to_string(index=False))

    print("\n📊 TOP 10 MUNICÍPIOS MAIS POPULOSOS")
    df_top = pd.read_sql("""
        SELECT nome, uf_sigla, populacao
        FROM municipios_populacao
        ORDER BY populacao DESC
        LIMIT 10
    """, conn)
    print(df_top.to_string(index=False))

    print("\n📊 ESTADOS COM MAIOR NÚMERO DE MUNICÍPIOS")
    df_uf = pd.read_sql("""
        SELECT uf_sigla, uf_nome, COUNT(*) AS total_municipios, SUM(populacao) AS populacao_total
        FROM municipios_populacao
        GROUP BY uf_sigla
        ORDER BY total_municipios DESC
        LIMIT 10
    """, conn)
    print(df_uf.to_string(index=False))

    conn.close()


# ── PIPELINE PRINCIPAL ────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("INICIANDO PIPELINE ETL — IBGE MUNICÍPIOS")
    logger.info("=" * 50)

    try:
        municipios, pop_data = extract()
        df = transform(municipios, pop_data)
        load(df)
        analisar()
        logger.success("Pipeline finalizado com sucesso!")
    except Exception as e:
        logger.error(f"Erro no pipeline: {e}")
        raise