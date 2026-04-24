import logging
from typing import Tuple

import os
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
            * EXCLUDE (quantity),
            quantity::INT AS quantity,
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
                EXTRACT(DAY FROM order_date)::INT AS day,
                EXTRACT(MONTH FROM order_date)::INT AS month,
                EXTRACT(YEAR FROM order_date)::INT AS year
""")

def build_and_populate_tables(relation: duckdb.DuckDBPyRelation) -> None:
    """
    Cria e popula as tabelas dimensão e fato do DW.
    Utiliza ON CONFLICT DO NOTHING para garantir idempotência —
    a função pode ser chamada múltiplas vezes sem duplicar dados.
    
    Args:
        relation: DuckDBPyRelation com os dados já tratados
    """

    logger_transform.info("Iniciando build do Data Warehouse...")

    # ------------------------------------------------------------------ #
    #  DIM_PRODUCT                                                         #
    # ------------------------------------------------------------------ #

    logger_transform.info("[1/5] Criando tabela dim_product...")
    duckdb.sql("""
        CREATE TABLE IF NOT EXISTS dim_product(
            product_id   VARCHAR(6)    PRIMARY KEY,
            product_name VARCHAR(30),
            unit_price   DECIMAL(12,2)
        )
    """)

    logger_transform.info("[1/5] Populando dim_product...")
    duckdb.sql("""
        INSERT INTO dim_product (product_id, product_name, unit_price)
        SELECT
            product_id,
            product_name,
            unit_price
        FROM relation
        ON CONFLICT (product_id) DO NOTHING
    """)

    total = duckdb.sql("SELECT COUNT(*) FROM dim_product").fetchone()[0]
    logger_transform.info(f"[1/5] dim_product OK — {total} produtos carregados.")

    # ------------------------------------------------------------------ #
    #  DIM_REPRESENTATIVE                                                  #
    # ------------------------------------------------------------------ #

    logger_transform.info("[2/5] Criando tabela dim_representative...")

    duckdb.sql("CREATE SEQUENCE IF NOT EXISTS seq_representative START 1")
    
    duckdb.sql("""
        CREATE TABLE IF NOT EXISTS dim_representative(
            rep_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_representative'),
            sales_rep VARCHAR(20) UNIQUE        -- UNIQUE necessário para o ON CONFLICT funcionar
        )
    """)

    logger_transform.info("[2/5] Populando dim_representative...")
    duckdb.sql("""
        INSERT INTO dim_representative (sales_rep)
        SELECT DISTINCT sales_rep              -- DISTINCT evita tentar inserir o mesmo rep duas vezes
        FROM relation
        ON CONFLICT (sales_rep) DO NOTHING
    """)

    total = duckdb.sql("SELECT COUNT(*) FROM dim_representative").fetchone()[0]
    logger_transform.info(f"[2/5] dim_representative OK — {total} representantes carregados.")

    # ------------------------------------------------------------------ #
    #  DIM_CUSTOMER                                                        #
    # ------------------------------------------------------------------ #

    logger_transform.info("[3/5] Criando tabela dim_customer...")
    duckdb.sql("""
        CREATE TABLE IF NOT EXISTS dim_customer(
            customer_id         VARCHAR(6)  PRIMARY KEY,
            customer_type       VARCHAR(15),
            first_purchase_date DATE,
            last_purchase_date  DATE,
            region              VARCHAR(10)
        )
    """)

    logger_transform.info("[3/5] Populando dim_customer...")
    duckdb.sql("""
        INSERT INTO dim_customer (customer_id, customer_type, first_purchase_date, last_purchase_date, region)
        SELECT
            customer_id,
            customer_type,
            first_purchase_date,
            last_purchase_date,
            region
        FROM relation
        ON CONFLICT (customer_id) DO NOTHING
    """)

    total = duckdb.sql("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    logger_transform.info(f"[3/5] dim_customer OK — {total} clientes carregados.")

    # ------------------------------------------------------------------ #
    #  DIM_TIME                                                            #
    # ------------------------------------------------------------------ #

    logger_transform.info("[4/5] Criando tabela dim_time...")

    duckdb.sql("CREATE SEQUENCE IF NOT EXISTS seq_time START 1")

    duckdb.sql("""
        CREATE TABLE IF NOT EXISTS dim_time(
            time_id    INTEGER     PRIMARY KEY DEFAULT nextval('seq_time'),
            date       DATE        UNIQUE,      -- UNIQUE para o ON CONFLICT funcionar por data
            day        INT,
            month      INT,
            year       INT,
            month_name VARCHAR(15)
        )
    """)

    logger_transform.info("[4/5] Populando dim_time...")
    duckdb.sql("""
        INSERT INTO dim_time (date, day, month, year, month_name)
        SELECT
            order_date,
            EXTRACT(day   FROM order_date)::INT   AS day,
            EXTRACT(month FROM order_date)::INT   AS month,
            EXTRACT(year  FROM order_date)::INT   AS year,
            strftime(order_date, '%B')            AS month_name
        FROM relation
        ON CONFLICT (date) DO NOTHING
    """)

    total = duckdb.sql("SELECT COUNT(*) FROM dim_time").fetchone()[0]
    logger_transform.info(f"[4/5] dim_time OK — {total} datas carregadas.")

    # ------------------------------------------------------------------ #
    #  FACT_ORDER                                                          #
    # ------------------------------------------------------------------ #

    logger_transform.info("[5/5] Criando tabela fact_order...")
    duckdb.sql("""
        CREATE TABLE IF NOT EXISTS fact_order(
            order_id     BIGINT        PRIMARY KEY,
            customer_id  VARCHAR(6)    REFERENCES dim_customer(customer_id),
            rep_id       INT           REFERENCES dim_representative(rep_id),
            product_id   VARCHAR(6)    REFERENCES dim_product(product_id),
            time_id      INT           REFERENCES dim_time(time_id),
            quantity     INT,
            discount_pct DECIMAL(3,2),
            sale_value   DECIMAL(12,2)
        )
    """)

    logger_transform.info("[5/5] Populando fact_order...")
    duckdb.sql("""
        INSERT INTO fact_order (order_id, customer_id, rep_id, product_id, time_id, quantity, discount_pct, sale_value)
        SELECT
            r.order_id,
            r.customer_id,
            dr.rep_id,         -- pega o rep_id gerado na dim_representative
            r.product_id,
            dt.time_id,        -- pega o time_id gerado na dim_time
            r.quantity,
            r.discount_pct,
            r.sale_value
        FROM relation r
        JOIN dim_representative dr ON dr.sales_rep = r.sales_rep
        JOIN dim_time           dt ON dt.date      = r.order_date
        ON CONFLICT (order_id) DO NOTHING
    """)

    total = duckdb.sql("SELECT COUNT(*) FROM fact_order").fetchone()[0]
    logger_transform.info(f"[5/5] fact_order OK — {total} pedidos carregados.")

    # ------------------------------------------------------------------ #

    logger_transform.info("Build do Data Warehouse concluído com sucesso!")

def save_parquet(tables: list[str], layer: str) -> None:
    """
    Grava uma lista de tabelas em arquivos Parquet no diretório data/{layer}/.

    Args:
        tables: Lista com os nomes das tabelas a serem gravadas.
        layer:  Camada do lakehouse onde os arquivos serão salvos (ex: 'silver', 'gold').

    Raises:
        duckdb.Error: Se ocorrer um erro ao gravar alguma tabela no formato Parquet.
    """
    logger_transform.info("Iniciando Gravação dos Dados")

    os.makedirs(f"data/{layer}", exist_ok=True)

    for table in tables:
        path = f"data/{layer}/{table}.parquet"
        try:
            logger_transform.info(f"Gravando dados da tabela {table}")
            duckdb.sql(f"""
                COPY {table}
                TO '{path}'
                (FORMAT PARQUET)
            """)
            logger_transform.info(f"Dados da tabela {table} gravados em {path}")
        except duckdb.Error as e:
            logger_transform.error(f"Erro ao gravar tabela {table}: {e}")
            raise

    logger_transform.info("Finalizando Gravação dos Dados")

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

        duckdb.register("electronics_sales_clean", relation_final)

        save_parquet(tables=['electronics_sales_clean'], layer='silver')

        duckdb.execute("DROP VIEW IF EXISTS electronics_sales_clean")

        build_and_populate_tables(relation_final)

        save_parquet(tables=["fact_order", "dim_customer", "dim_product", "dim_time", "dim_representative"], layer="gold")

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
    FILEPATH = "data/bronze/electronics_sales_raw.csv"

    df = run(FILEPATH)

#TODO: CORRIGIR VALIDATE