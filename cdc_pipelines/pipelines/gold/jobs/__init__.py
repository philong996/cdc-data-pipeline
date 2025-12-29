"""Table processors for gold layer analytics tables."""

from cdc_pipelines.pipelines.gold.jobs.product_performance_daily import build_product_performance_daily
from cdc_pipelines.pipelines.gold.jobs.customer_order_summary import build_customer_order_summary
from cdc_pipelines.pipelines.gold.jobs.order_hourly_patterns import build_order_hourly_patterns

__all__ = [
    "build_product_performance_daily",
    "build_customer_order_summary",
    "build_order_hourly_patterns",
]
