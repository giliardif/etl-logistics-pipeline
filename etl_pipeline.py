"""
ETL Pipeline - Logística / Supply Chain
========================================
Stack: Python · Pandas · Polars
Camadas: Bronze → Silver → Gold
Autor: Giliardi | Nexus Tech BI
"""

import pandas as pd
import polars as pl
import json
import os
import logging
from datetime import datetime
from pathlib import Path

# ─── Configuração ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

for d in [RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ─── EXTRAÇÃO ────────────────────────────────────────────────────────────────

def extract_orders(source_path: str | None = None) -> pd.DataFrame:
    """
    Extrai pedidos de logística.
    Se source_path for None, gera dados sintéticos para demo.
    """
    log.info("📥 [EXTRACT] Iniciando ingestão de pedidos...")

    if source_path and Path(source_path).exists():
        df = pd.read_csv(source_path)
        log.info(f"  ↳ Lidos {len(df)} registros de {source_path}")
        return df

    # Dados sintéticos representativos
    import numpy as np
    np.random.seed(42)
    n = 500

    status_options = ["Entregue", "Em Trânsito", "Atrasado", "Cancelado", "Pendente"]
    carriers = ["Correios", "Jadlog", "Sequoia", "Braspress", "Total Express"]
    regions = ["Nordeste", "Sudeste", "Sul", "Centro-Oeste", "Norte"]

    df = pd.DataFrame({
        "order_id": [f"PED-{1000 + i}" for i in range(n)],
        "created_at": pd.date_range("2024-01-01", periods=n, freq="3h").strftime("%Y-%m-%d %H:%M:%S"),
        "delivered_at": [
            (pd.Timestamp("2024-01-01") + pd.Timedelta(hours=3 * i + np.random.randint(24, 240))).strftime("%Y-%m-%d %H:%M:%S")
            if np.random.random() > 0.15 else None
            for i in range(n)
        ],
        "status": np.random.choice(status_options, n, p=[0.55, 0.20, 0.12, 0.08, 0.05]),
        "carrier": np.random.choice(carriers, n),
        "region": np.random.choice(regions, n),
        "weight_kg": np.round(np.random.exponential(3, n), 2),
        "freight_cost": np.round(np.random.uniform(15, 350, n), 2),
        "sla_days": np.random.choice([3, 5, 7, 10], n),
        "customer_id": [f"CLI-{np.random.randint(100, 999)}" for _ in range(n)],
    })

    log.info(f"  ↳ {len(df)} pedidos sintéticos gerados")
    return df


def save_bronze(df: pd.DataFrame) -> Path:
    """Camada Bronze: dados brutos com timestamp de ingestão."""
    log.info("🥉 [BRONZE] Salvando dados brutos...")
    df["ingested_at"] = datetime.now().isoformat()
    path = BRONZE_DIR / f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"  ↳ Salvo em {path}")
    return path


# ─── TRANSFORMAÇÃO ───────────────────────────────────────────────────────────

def transform_silver(bronze_path: Path) -> pl.DataFrame:
    """
    Camada Silver: limpeza, tipagem, remoção de nulos críticos.
    Utiliza Polars para performance.
    """
    log.info("🥈 [SILVER] Transformando dados com Polars...")

    df = pl.read_parquet(bronze_path)

    df = (
        df
        .with_columns([
            pl.col("created_at").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            pl.col("delivered_at").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
            pl.col("weight_kg").cast(pl.Float64),
            pl.col("freight_cost").cast(pl.Float64),
        ])
        .filter(pl.col("order_id").is_not_null())
        .filter(pl.col("status").is_not_null())
        .with_columns([
            pl.when(pl.col("weight_kg") < 0).then(0).otherwise(pl.col("weight_kg")).alias("weight_kg"),
            pl.when(pl.col("freight_cost") < 0).then(0).otherwise(pl.col("freight_cost")).alias("freight_cost"),
        ])
    )

    path = SILVER_DIR / "orders_clean.parquet"
    df.write_parquet(path)
    log.info(f"  ↳ {len(df)} registros limpos. Salvo em {path}")
    return df


def transform_gold(df_silver: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """
    Camada Gold: agregações analíticas prontas para consumo.
    Retorna múltiplas tabelas de negócio.
    """
    log.info("🥇 [GOLD] Gerando tabelas analíticas...")

    # 1. KPIs por transportadora
    df_with_lead = df_silver.with_columns([
        ((pl.col("delivered_at") - pl.col("created_at")).dt.total_hours() / 24)
        .alias("lead_time_days")
    ])

    carrier_kpis = (
        df_with_lead
        .group_by("carrier")
        .agg([
            pl.len().alias("total_orders"),
            (pl.col("status") == "Entregue").sum().alias("delivered"),
            (pl.col("status") == "Atrasado").sum().alias("delayed"),
            pl.col("freight_cost").mean().round(2).alias("avg_freight_cost"),
            pl.col("lead_time_days").mean().round(1).alias("avg_lead_time_days"),
        ])
        .with_columns([
            (pl.col("delivered") / pl.col("total_orders") * 100).round(1).alias("delivery_rate_pct"),
            (pl.col("delayed") / pl.col("total_orders") * 100).round(1).alias("delay_rate_pct"),
        ])
        .sort("total_orders", descending=True)
    )

    # 2. Performance por região
    region_summary = (
        df_silver
        .group_by("region")
        .agg([
            pl.len().alias("total_orders"),
            pl.col("freight_cost").sum().round(2).alias("total_freight_cost"),
            pl.col("weight_kg").mean().round(2).alias("avg_weight_kg"),
            (pl.col("status") == "Atrasado").sum().alias("delayed_count"),
        ])
        .sort("total_orders", descending=True)
    )

    # 3. Série temporal mensal
    monthly_trend = (
        df_silver
        .with_columns(pl.col("created_at").dt.strftime("%Y-%m").alias("month"))
        .group_by("month")
        .agg([
            pl.len().alias("orders"),
            pl.col("freight_cost").sum().round(2).alias("revenue"),
            (pl.col("status") == "Entregue").sum().alias("delivered"),
        ])
        .sort("month")
    )

    tables = {
        "carrier_kpis": carrier_kpis,
        "region_summary": region_summary,
        "monthly_trend": monthly_trend,
    }

    for name, tbl in tables.items():
        path = GOLD_DIR / f"{name}.parquet"
        tbl.write_parquet(path)
        log.info(f"  ↳ {name}: {len(tbl)} linhas → {path}")

    return tables


# ─── CARGA / RELATÓRIO ───────────────────────────────────────────────────────

def load_report(gold_tables: dict[str, pl.DataFrame]) -> None:
    """Exporta relatório consolidado em JSON para consumo downstream."""
    log.info("📤 [LOAD] Exportando relatório final...")

    report = {
        "generated_at": datetime.now().isoformat(),
        "pipeline": "ETL Logística · Bronze → Silver → Gold",
        "tables": {
            name: tbl.to_pandas().to_dict(orient="records")
            for name, tbl in gold_tables.items()
        },
    }

    out_path = GOLD_DIR / "report.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    log.info(f"  ↳ Relatório salvo em {out_path}")


# ─── ORQUESTRAÇÃO ────────────────────────────────────────────────────────────

def run_pipeline(source_path: str | None = None) -> None:
    log.info("🚀 Pipeline ETL Logística iniciado")
    log.info("=" * 60)

    # 1. Extract
    df_raw = extract_orders(source_path)

    # 2. Bronze
    bronze_path = save_bronze(df_raw)

    # 3. Silver
    df_silver = transform_silver(bronze_path)

    # 4. Gold
    gold_tables = transform_gold(df_silver)

    # 5. Load
    load_report(gold_tables)

    log.info("=" * 60)
    log.info("✅ Pipeline concluído com sucesso!")
    log.info(f"   Bronze: {BRONZE_DIR}")
    log.info(f"   Silver: {SILVER_DIR}")
    log.info(f"   Gold:   {GOLD_DIR}")


if __name__ == "__main__":
    run_pipeline()
