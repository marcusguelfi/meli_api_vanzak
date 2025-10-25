# src/product_ads_metrics.py
from __future__ import annotations

from enum import Enum
from typing import Iterable, List


class Aggregation(str, Enum):
    DAILY = "DAILY"
    SUMMARY = "SUMMARY"


# Métricas válidas para nível CAMPANHA (podem incluir shares/sov)
METRICS_CAMPAIGN: List[str] = [
    "clicks", "prints", "ctr", "cost", "cpc", "acos",
    "organic_units_quantity", "organic_units_amount", "organic_items_quantity",
    "direct_items_quantity", "indirect_items_quantity", "advertising_items_quantity",
    "cvr", "roas", "sov",
    "direct_units_quantity", "indirect_units_quantity", "units_quantity",
    "direct_amount", "indirect_amount", "total_amount",
    "impression_share", "top_impression_share",
    "lost_impression_share_by_budget", "lost_impression_share_by_ad_rank",
    "acos_benchmark",
]

# Métricas válidas para nível ANÚNCIO (sem shares/sov)
METRICS_ADS: List[str] = [
    "clicks", "prints", "ctr", "cost", "cpc", "acos",
    "organic_units_quantity", "organic_units_amount", "organic_items_quantity",
    "direct_items_quantity", "indirect_items_quantity", "advertising_items_quantity",
    "cvr", "roas",
    "direct_units_quantity", "indirect_units_quantity", "units_quantity",
    "direct_amount", "indirect_amount", "total_amount",
]

# Limite de métricas por request que algumas APIs impõem
API_METRIC_LIMIT: int = 20


def validate_metrics_any(metrics: Iterable[str]) -> List[str]:
    """
    Valida contra o conjunto total (campanha ∪ anúncio).
    """
    allowed = set(METRICS_CAMPAIGN) | set(METRICS_ADS)
    metrics = list(metrics)
    missing = [m for m in metrics if m not in allowed]
    if missing:
        raise ValueError(f"Métricas inválidas (fora do conjunto permitido): {missing}")
    return metrics


def validate_metrics_for(level: str, metrics: Iterable[str]) -> List[str]:
    """
    Valida métricas específicas por nível:
      - level='campaign' → valida em METRICS_CAMPAIGN
      - level='ads'      → valida em METRICS_ADS
    """
    metrics = list(metrics)
    if level.lower() == "campaign":
        allowed = set(METRICS_CAMPAIGN)
    elif level.lower() == "ads":
        allowed = set(METRICS_ADS)
    else:
        raise ValueError(f"Nível desconhecido: {level!r} (use 'campaign' ou 'ads')")

    missing = [m for m in metrics if m not in allowed]
    if missing:
        raise ValueError(f"Métricas inválidas para {level}: {missing}")
    return metrics


def chunk_metrics(metrics: Iterable[str], size: int = API_METRIC_LIMIT, *, level: str | None = None) -> List[List[str]]:
    """
    Divide as métricas em grupos de até `size`.
    - Se `level` for informado, valida contra o conjunto do nível.
    - Caso contrário, valida contra o conjunto total.
    """
    if level:
        vals = validate_metrics_for(level, metrics)
    else:
        vals = validate_metrics_any(metrics)
    return [vals[i:i + size] for i in range(0, len(vals), size)]


# Schemas recomendados para congelar cabeçalho dos CSVs
SCHEMA_CAMPAIGN_DAILY: List[str] = [
    "advertiser_id", "site_id", "date", "campaign_id", "campaign_name",
    *METRICS_CAMPAIGN,
]

SCHEMA_ADS_DAILY: List[str] = [
    "advertiser_id", "site_id", "date",
    "campaign_id", "campaign_name",
    "ad_id", "item_id", "item_title", "seller_sku", "status",
    *METRICS_ADS,
]

__all__ = [
    "Aggregation",
    "METRICS_CAMPAIGN", "METRICS_ADS",
    "API_METRIC_LIMIT",
    "validate_metrics_any", "validate_metrics_for", "chunk_metrics",
    "SCHEMA_CAMPAIGN_DAILY", "SCHEMA_ADS_DAILY",
]
