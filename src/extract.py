import logging
from typing import Tuple

import duckdb
import pandas as pd


# ── Configuração de logging ───────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger_transform = logging.getLogger("TRANSFORM")


# ── Funções do pipeline ───────────────────────────────────────────────────────

def load_and_clean(filepath: str) -> duckdb.DuckDBPyRelation:
    """
    Lê o CSV bruto e aplica as transformações principais via DuckDB:
        - Filtros de qualidade (quantity, unit_price, discount_pct, chaves primárias)
        - Cast de colunas de data para TIMESTAMP
        - Cálculo da coluna derivada sale_value
        - Deduplicação por order_id (mantém o registro mais recente)

    Args:
        filepath: Caminho para o arquivo CSV de entrada.

    Returns:
        DuckDBPyRelation com os dados limpos e deduplicados.

    Raises:
        duckdb.IOException: Se houver erro na leitura do arquivo.
    """
    logger_transform.info("Iniciando leitura do arquivo: %s", filepath)

    relation = duckdb.sql(f"""
    WITH base AS (
        SELECT
            * EXCLUDE (order_date, last_purchase_date, first_purchase_date),
            order_date::TIMESTAMP           AS order_date,
            last_purchase_date::TIMESTAMP   AS last_purchase_date,
            first_purchase_date::TIMESTAMP  AS first_purchase_date,
            ROUND(quantity * unit_price * (1 - discount_pct), 2) AS sale_value
        FROM '{filepath}'
        WHERE quantity      > 0
          AND unit_price    > 0
          AND discount_pct  BETWEEN 0 AND 1
          AND order_id      IS NOT NULL
          AND customer_id   IS NOT NULL
          AND product_id    IS NOT NULL
    ),

    deduplicado AS (
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY order_date DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    )

    SELECT * FROM deduplicado
""")

    logger_transform.info("Leitura e limpeza concluídas com sucesso.")
    return relation


def normalize_strings(relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyConnection:
    """
    Detecta dinamicamente as colunas VARCHAR e aplica TRIM + INITCAP via DuckDB,
    mantendo todo o processamento dentro do engine sem uso de pandas.apply().

    Args:
        relation: DuckDBPyRelation resultante da etapa de limpeza.

    Returns:
        DataFrame pandas com strings normalizadas.
    """
    logger_transform.info("Iniciando normalização de strings.")

    # Detecta colunas string dinamicamente a partir dos tipos da relation
    tipos = duckdb.sql("SELECT column_name, column_type FROM (DESCRIBE relation)").fetchall()
    
    colunas_str = [nome for nome, tipo in tipos if tipo == "VARCHAR"]
    outras      = [nome for nome, tipo in tipos if tipo != "VARCHAR"]

    logger_transform.info("Colunas VARCHAR detectadas")

    # Monta SELECT aplicando TRIM + INITCAP apenas nas colunas string
    select_str = [
    f'UPPER(SUBSTR(TRIM("{col}"), 1, 1)) || LOWER(SUBSTR(TRIM("{col}"), 2)) AS "{col}"'
    for col in colunas_str
]
    select_final = ", ".join(outras + select_str)

    df = duckdb.sql(f"SELECT {select_final} FROM relation")

    logger_transform.info("Normalização concluída")
    return df


def validate(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Executa validações de integridade no DataFrame final:
        - Ausência de duplicatas em order_id
        - sale_value não negativo
        - Chaves primárias sem valores nulos (order_id, customer_id, product_id)

    Args:
        df: DataFrame final após todas as transformações.

    Returns:
        Tupla (sucesso: bool, mensagem: str) indicando resultado da validação.
    """
    logger_transform.info("Iniciando validações de integridade.")

    if df["order_id"].duplicated().any():
        return False, "Duplicatas encontradas em order_id."

    if not df["sale_value"].ge(0).all():
        return False, "Valores negativos encontrados em sale_value."

    chaves = ["order_id", "customer_id", "product_id"]
    if not df[chaves].notna().all().all():
        return False, "Valores nulos encontrados nas chaves primárias."

    return True, "Todas as validações passaram com sucesso."

def create_date(relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    return relation.project("""
                *,
                EXTRACT(DAY FROM order_date) AS day,
                EXTRACT(MONTH FROM order_date) AS month,
                EXTRACT(YEAR FROM order_date) AS year
""")
    

def build_dims(relation: duckdb.DuckDBPyRelation) -> None:
    teste = duckdb.sql("""
        CREATE TABLE dim_teste AS
                    SELECT 
                        day,
                        month    
                        year
""")
    
    print(teste)

def build_fact(relation: duckdb.DuckDBPyRelation) -> None:
    ...

def run(filepath: str) -> pd.DataFrame:
    """
    Orquestra o pipeline completo de transformação:
        1. Leitura e limpeza (load_and_clean)
        2. Normalização de strings (normalize_strings)
        3. Validação de integridade (validate)

    Args:
        filepath: Caminho para o arquivo CSV de entrada.

    Returns:
        DataFrame final transformado e validado.

    Raises:
        RuntimeError: Se alguma validação de integridade falhar.
        Exception: Propaga exceções de leitura ou transformação com log de erro.
    """
    logger_transform.info("=" * 60)
    logger_transform.info("Iniciando pipeline de transformação de vendas.")
    logger_transform.info("=" * 60)

    try:
        # Etapa 1 — leitura, limpeza e deduplicação
        relation = load_and_clean(filepath)

        relation = create_date(relation)

        # Etapa 2 — normalização de strings
        relation_final = normalize_strings(relation)

        build_dims(relation_final)

        # Etapa 3 — validação de integridade
        # ok, mensagem = validate(relation_final)
        # if not ok:
        #     logger_transform.error("Falha na validação: %s", mensagem)
        #     raise RuntimeError(f"Validação falhou: {mensagem}")

        # logger_transform.info(mensagem)
        logger_transform.info("Pipeline concluído. Total de registros: %d", len(relation_final))
        # logger_transform.info("Colunas disponíveis: %s", list(relation_final.columns))
        logger_transform.info("=" * 60)

        return relation_final

    except Exception as exc:
        logger_transform.exception("Erro inesperado durante o pipeline: %s", exc)
        raise




# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    FILEPATH = "data/electronics_sales_raw.csv"

    df = run(FILEPATH)
    print(df.columns)