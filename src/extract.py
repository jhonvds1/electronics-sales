import duckdb


# Carrega o CSV
resultado = duckdb.sql("SELECT * FROM 'data/electronics_sales_raw.csv'")

# Transformação
resultado = duckdb.sql("""
    SELECT 
        *,
        ROUND(quantity * unit_price * (1 - discount_pct), 2) AS sale_value
    FROM resultado
    WHERE quantity > 0
      AND unit_price > 0
      AND discount_pct BETWEEN 0 AND 1
""")

resultado = duckdb.sql("""
    SELECT
        * EXCLUDE (order_date, last_purchase_date, first_purchase_date),
        order_date::TIMESTAMP           AS order_date,
        last_purchase_date::TIMESTAMP   AS last_purchase_date,
        first_purchase_date::TIMESTAMP  AS first_purchase_date
    FROM resultado
""")

resultado = duckdb.sql("""
    SELECT * FROM resultado where order_id is not null and customer_id is not null and product_id is not null
""")

resultado = duckdb.sql("""
    SELECT * EXCLUDE (rn)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY order_date DESC
            ) AS rn
        FROM resultado
    )
    WHERE rn = 1
""").to_df()

resultado = resultado.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

resultado = resultado.apply(lambda x: x.str.title() if x.dtype == 'object' else x)

print(resultado.select_dtypes(include='object'))

print(resultado)