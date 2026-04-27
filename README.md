# 🛒 Electronics Sales ETL Pipeline

Pipeline ETL construído em Python com DuckDB para processar dados brutos de vendas de eletrônicos, aplicar transformações de qualidade e estruturar os dados em um Data Warehouse com camadas **Bronze → Silver → Gold**.

---

## 📌 Visão Geral

O pipeline lê um arquivo CSV bruto, realiza limpeza, deduplicação, normalização de strings e popula um modelo dimensional (star schema) com tabelas de dimensão e fato. Os dados são persistidos em formato **Parquet** em duas camadas do lakehouse.

```
data/bronze/electronics_sales_raw.csv
        │
        ▼
   [ EXTRACT ]  → Leitura, filtros de qualidade, cast de tipos, cálculo de sale_value
        │
        ▼
  [ TRANSFORM ] → Normalização de strings, extração de datas, build do Data Warehouse
        │
        ▼
    [ LOAD ]    → Parquet (Silver) + Parquet (Gold)
```

---

## 🗂️ Estrutura do Projeto

```
.
├── data/
│   ├── bronze/          # ⬇️ Baixar manualmente (dados brutos de entrada)
│   ├── silver/          # Gerado pelo pipeline
│   └── gold/            # Gerado pelo pipeline
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── src/
│   └── main.py
├── diagrama_db.png
├── requirements.txt
├── .gitignore
└── README.md
```

---

## ⚙️ Etapas do Pipeline

### 1. Extract — `load_and_clean()`
- Leitura do CSV diretamente via DuckDB (sem Pandas)
- Filtros de qualidade: `quantity > 0`, `unit_price > 0`, `discount_pct BETWEEN 0 AND 1`, chaves primárias não nulas
- Cálculo da coluna derivada: `sale_value = quantity × unit_price × (1 − discount_pct)`
- Deduplicação por `order_id`, mantendo o registro mais recente

### 2. Transform — `normalize_strings()` + `create_date()`
- Detecção dinâmica de colunas `VARCHAR` para aplicar `TRIM + INITCAP`
- Todo o processamento é feito dentro do engine DuckDB (sem `pandas.apply()`)
- Extração de `day`, `month` e `year` a partir de `order_date`

### 3. Load — `build_and_populate_tables()` + `save_parquet()`
- Criação e população das tabelas do DW com `ON CONFLICT DO NOTHING` (idempotência garantida)
- Exportação em Parquet para as camadas **silver** e **gold**

---

## 🏗️ Modelo de Dados (Star Schema)

```
                    ┌──────────────────────┐
                    │      dim_time        │
                    │──────────────────────│
                    │ PK: time_id  INT     │
                    │     date     DATE    │
                    │     day      INT     │
                    │     month    INT     │
                    │     year     INT     │
                    │     month_name VARCHAR│
                    └──────────┬───────────┘
                               │
┌─────────────────────┐  ┌─────▼──────────────────┐  ┌─────────────────────────┐
│    dim_customer     │  │      fact_order         │  │      dim_product        │
│─────────────────────│  │────────────────────────-│  │─────────────────────────│
│ PK: customer_id     ├──┤ PK: order_id    BIGINT  ├──┤ PK: product_id  VARCHAR │
│     customer_type   │  │ FK: customer_id VARCHAR │  │     product_name VARCHAR │
│     first_purchase_ │  │ FK: rep_id      INT     │  │     unit_price  DECIMAL  │
│       date DATE     │  │ FK: product_id  VARCHAR │  └─────────────────────────┘
│     last_purchase_  │  │ FK: time_id     INT     │
│       date DATE     │  │     quantity    INT     │  ┌─────────────────────────┐
│     region  VARCHAR │  │     discount_pct DECIMAL├──┤   dim_representative   │
└─────────────────────┘  │     sale_value  DECIMAL │  │─────────────────────────│
                         └─────────────────────────┘  │ PK: rep_id     INT      │
                                                       │     sales_rep  VARCHAR  │
                                                       └─────────────────────────┘
```

| Tabela | Descrição |
|---|---|
| `fact_order` | Fatos de pedidos: quantidade, desconto, valor da venda |
| `dim_customer` | Dimensão cliente: tipo, região, datas de compra |
| `dim_product` | Dimensão produto: nome e preço unitário |
| `dim_time` | Dimensão tempo: dia, mês, ano, nome do mês |
| `dim_representative` | Dimensão representante de vendas |

---

## 🚀 Como Executar

### Pré-requisitos

- [Docker](https://www.docker.com/) instalado
- Arquivo CSV de entrada baixado em `data/bronze/` → [📥 Download aqui](https://drive.google.com/drive/u/0/folders/1KQuCJsWvw8vVbbiWZAxPJhIxRvhKDft2)

### Execução

```bash
cd docker
docker compose up --build
```

O pipeline lê o arquivo em `data/bronze/electronics_sales_raw.csv` e grava os resultados automaticamente nas pastas `silver/` e `gold/`.

### Exemplo de entrada (CSV)

```
order_id,customer_id,product_id,sales_rep,order_date,quantity,unit_price,discount_pct,...
```

---

## 📋 Logging

O pipeline utiliza o módulo `logging` do Python com quatro loggers distintos:

| Logger | Responsável por |
|---|---|
| `EXTRACT` | Leitura e limpeza dos dados |
| `TRANSFORM` | Normalização, build do DW |
| `LOAD` | Gravação dos arquivos Parquet |
| `MAIN` | Orquestração geral do pipeline |

---

## 🛠️ Tecnologias

- **Python 3.11+**
- **DuckDB** — processamento SQL in-process de alta performance
- **Pandas** — estruturas de dados auxiliares
- **Parquet** — formato colunar para armazenamento eficiente

---

## ✨ Destaques Técnicos

- Processamento 100% em memória via DuckDB, sem dependência de banco externo
- Pipeline idempotente: pode ser reexecutado sem duplicar dados (`ON CONFLICT DO NOTHING`)
- Detecção dinâmica de colunas `VARCHAR` para normalização de strings
- Arquitetura Medallion (Bronze → Silver → Gold)
- Logging estruturado por camada do pipeline

---

## 🙌 Créditos

Este projeto foi desenvolvido como parte da comunidade **Dados Por Todos**.

- **Dados Por Todos** — [@dadosportodos](https://www.linkedin.com/company/dadosportodos)
- **Luiza Vieira** — [@vbluuiza](https://www.linkedin.com/in/vbluuiza)
- **Laura Almeida** — [@lauraalmeidaaa](https://www.linkedin.com/in/lauraalmeidaaa)
