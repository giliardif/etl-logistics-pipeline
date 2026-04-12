# 🚛 ETL Pipeline — Logística & Supply Chain

> Pipeline de dados em camadas **Bronze → Silver → Gold** para análise de operações logísticas, construído com Python, Pandas e Polars.

---

## 📐 Arquitetura

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

## 🧰 Stack

| Camada | Tecnologia | Finalidade |
|--------|-----------|------------|
| Extração | `pandas` | Leitura de CSV / geração de dados sintéticos |
| Armazenamento | `parquet` | Formato colunar eficiente |
| Transformação | `polars` | Performance em limpeza e joins |
| Orquestração | Python nativo | Script modular e extensível |
| Output | JSON + Parquet | Consumo por dashboards e APIs |

## 📊 Tabelas Gold geradas

| Tabela | Descrição |
|--------|-----------|
| `carrier_kpis` | Taxa de entrega, atraso e custo médio por transportadora |
| `region_summary` | Volume, receita e peso médio por região |
| `monthly_trend` | Série temporal de pedidos e receita por mês |

## 🚀 Como executar

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/etl-logistics-pipeline
cd etl-logistics-pipeline

# 2. Instale as dependências
pip install pandas polars pyarrow

# 3. Execute o pipeline (dados sintéticos incluídos)
python etl_pipeline.py

# 4. Ou passe seu próprio CSV
python -c "from etl_pipeline import run_pipeline; run_pipeline('seu_arquivo.csv')"
```

## 📁 Estrutura do projeto

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

## 🔄 Próximos passos sugeridos

- [ ] Orquestração com **Apache Airflow** ou **Prefect**
- [ ] Carga em **BigQuery** ou **DuckDB**
- [ ] Dashboard com **Streamlit** ou **Power BI**
- [ ] Alertas automáticos para pedidos com alto índice de atraso

---

## 👤 Autor

**Giliardi** — Data & AI Engineer  
[LinkedIn](#) · [GitHub](#)

> Projeto desenvolvido para fins de portfólio e estudo de arquitetura de dados em camadas (Medallion Architecture).
