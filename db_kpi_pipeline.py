# app/services/db_kpi_pipeline.py

from datetime import datetime, timedelta
from sqlalchemy import func, desc
from loguru import logger

from app.db.db_config import SessionLocal
from app.db.models import Customer, Order


def run_db_kpi_pipeline():
    """
    Compute KPIs using SQLAlchemy ORM.
    Matches EXACT behavior of pandas_pipeline & spark_pipeline:

    ✅ one row per ORDER (order-level dedupe using GROUP BY order_id)
    ✅ region revenue = sum(total_amount per order)
    ✅ repeat customers based on distinct order_ids
    ✅ top spenders use last 30 days of order-level data
    ✅ monthly order trend counts DISTINCT order_ids
    """

    logger.info("🚀 Starting DB KPI Pipeline...")
    session = SessionLocal()

    # ------------------------------------------------------------
    # ✅ Build ORDER-LEVEL subquery
    #     - Dedupes orders based on order_id
    #     - Uses MAX(total_amount) per order (same as Pandas/Spark)
    # ------------------------------------------------------------
    order_level_subq = (
        session.query(
            Order.order_id.label("order_id"),
            Order.mobile_number.label("mobile_number"),
            func.max(Order.total_amount).label("order_amount"),
            func.max(Order.order_date_time).label("order_date_time"),
        )
        .group_by(Order.order_id)
        .subquery()
    )

    # ------------------------------------------------------------
    # ✅ KPI 1 — Monthly Order Trend
    # ------------------------------------------------------------
    monthly_rows = (
        session.query(
            func.date_format(Order.order_date_time, "%Y-%m").label("year_month"),
            func.count(func.distinct(Order.order_id)).label("order_count"),
        )
        .group_by("year_month")
        .order_by("year_month")
        .all()
    )

    monthly_order_trend = {
        row.year_month: row.order_count for row in monthly_rows
    }

    # ------------------------------------------------------------
    # ✅ KPI 2 — Region-wise Revenue (order-level)
    # ------------------------------------------------------------
    region_rows = (
        session.query(
            Customer.region.label("region"),
            func.sum(order_level_subq.c.order_amount).label("revenue"),
        )
        .join(
            Customer,
            Customer.mobile_number == order_level_subq.c.mobile_number,
        )
        .group_by(Customer.region)
        .order_by(desc("revenue"))
        .all()
    )

    region_revenue = {
        row.region: float(row.revenue) for row in region_rows
    }

    # ------------------------------------------------------------
    # ✅ KPI 3 — Repeat Customers
    # ------------------------------------------------------------
    repeat_rows = (
        session.query(
            Customer.customer_id.label("customer_id"),
            func.count(func.distinct(Order.order_id)).label("order_count"),
        )
        .join(Order, Order.mobile_number == Customer.mobile_number)
        .group_by(Customer.customer_id)
        .having(func.count(func.distinct(Order.order_id)) > 1)
        .all()
    )

    repeat_customer_ids = [row.customer_id for row in repeat_rows]

    repeat_customers = {
        "count": len(repeat_customer_ids),
        "repeat_customers": repeat_customer_ids,
    }

    # ------------------------------------------------------------
    # ✅ KPI 4 — Top Spenders (last 30 days, order-level)
    # ------------------------------------------------------------
    cutoff_date = datetime.today() - timedelta(days=30)

    top_rows = (
        session.query(
            Customer.customer_id,
            Customer.customer_name,
            func.sum(order_level_subq.c.order_amount).label("amount"),
        )
        .join(
            Customer,
            Customer.mobile_number == order_level_subq.c.mobile_number,
        )
        .filter(order_level_subq.c.order_date_time >= cutoff_date)
        .group_by(Customer.customer_id, Customer.customer_name)
        .order_by(desc("amount"))
        .all()
    )

    top_spenders_last_30_days = [
        {
            "customer_id": row.customer_id,
            "customer_name": row.customer_name,
            "amount": float(row.amount),
        }
        for row in top_rows
    ]

    session.close()
    logger.success("🎉 DB KPI Pipeline completed!")

    return {
        "monthly_order_trend": monthly_order_trend,
        "region_revenue": region_revenue,
        "repeat_customers": repeat_customers,
        "top_spenders_last_30_days": top_spenders_last_30_days,
    }


if __name__ == "__main__":
    result = run_db_kpi_pipeline()
    print(result)
