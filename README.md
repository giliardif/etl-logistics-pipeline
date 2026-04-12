# ETL Pipeline — Logística & Supply Chain

[![Pipeline](https://img.shields.io/badge/pipeline-Bronze→Silver→Gold-f0a500?style=flat-square&logo=python&logoColor=white)](https://giliardif.github.io/etl-logistics-pipeline/)
[![Stack](https://img.shields.io/badge/stack-Python%20·%20Pandas%20·%20Polars-1daf82?style=flat-square)](https://github.com/giliardif/etl-logistics-pipeline)
[![Dashboard](https://img.shields.io/badge/dashboard-ao%20vivo-e05555?style=flat-square)](https://giliardif.github.io/etl-logistics-pipeline/)

> Pipeline de dados em camadas **Bronze → Silver → Gold** para análise de operações logísticas, construído com Python, Pandas e Polars.

🔗 **Dashboard ao vivo:** [giliardif.github.io/etl-logistics-pipeline](https://giliardif.github.io/etl-logistics-pipeline/)

---

## Arquitetura

```
Fonte de Dados (CSV / API / ERP)
        │
        ▼
  ┌─────────────┐
  │   BRONZE    │  ← Dados brutos + timestamp de ingestão (.parquet)
  └──────┬──────┘
         │  Polars: limpeza, tipagem, filtros
         ▼
  ┌─────────────┐
  │   SILVER    │  ← Dados limpos e padronizados (.parquet)
  └──────┬──────┘
         │  Agregações de negócio
         ▼
  ┌─────────────┐
  │    GOLD     │  ← KPIs prontos para BI / dashboards (.parquet + .json)
  └─────────────┘
```

## Stack

| Camada | Tecnologia | Finalidade |
|--------|-----------|------------|
| Extração | `pandas` | Leitura de CSV / geração de dados sintéticos |
| Armazenamento | `parquet` | Formato colunar eficiente |
| Transformação | `polars` | Performance em limpeza e joins |
| Orquestração | Python nativo | Script modular e extensível |
| Output | JSON + Parquet | Consumo por dashboards e APIs |

## Tabelas Gold geradas

| Tabela | Descrição |
|--------|-----------|
| `carrier_kpis` | Taxa de entrega, atraso e custo médio por transportadora |
| `region_summary` | Volume, receita e peso médio por região |
| `monthly_trend` | Série temporal de pedidos e receita por mês |

## Como executar

```bash
# 1. Clone o repositório
git clone https://github.com/giliardif/etl-logistics-pipeline
cd etl-logistics-pipeline

# 2. Instale as dependências
pip install pandas polars pyarrow

# 3. Execute o pipeline (dados sintéticos incluídos)
python etl_pipeline.py

# 4. Ou passe seu próprio CSV
python -c "from etl_pipeline import run_pipeline; run_pipeline('seu_arquivo.csv')"
```

## Estrutura do projeto

```
etl-logistics-pipeline/
├── etl_pipeline.py        # Script principal
├── README.md
└── data/
    ├── raw/               # Arquivos de entrada originais
    ├── bronze/            # Dados brutos + metadados de ingestão
    ├── silver/            # Dados limpos e tipados
    └── gold/              # KPIs e relatórios finais
        ├── carrier_kpis.parquet
        ├── region_summary.parquet
        ├── monthly_trend.parquet
        └── report.json
```

## Próximos passos sugeridos

- [ ] Orquestração com **Apache Airflow** ou **Prefect**
- [ ] Carga em **BigQuery** ou **DuckDB**
- [ ] Dashboard com **Streamlit** ou **Power BI**
- [ ] Alertas automáticos para pedidos com alto índice de atraso

---

## Autor

**Giliardi** — Data & AI Engineer  
[LinkedIn](#) · [GitHub](#)

> Projeto desenvolvido para fins de portfólio e estudo de arquitetura de dados em camadas (Medallion Architecture).
