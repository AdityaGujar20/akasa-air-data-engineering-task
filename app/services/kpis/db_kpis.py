import pandas as pd
import plotly.express as px
from sqlalchemy.orm import Session
from typing import List

from app.db.db_config import get_engine, SessionLocal
from app.db.models import Customer, Order
from app.services.kpis.base_kpis import KPICard, KPIChart, KPIBundle


# ✅ Helper to run ORM queries safely
def get_session() -> Session:
    return SessionLocal()


def compute_db_bundle() -> KPIBundle:
    """
    Computes all KPIs using ORM (no raw SQL).
    Returns KPIBundle(cards=[...], charts=[...])
    """

    session = get_session()

    # -------------------------------
    # ✅ BASIC METRICS
    # -------------------------------

    total_customers = session.query(Customer).count()
    total_orders = session.query(Order).count()

    revenue = session.query(Order.order_amount).all()
    revenue = sum([float(r[0]) for r in revenue]) if revenue else 0.0

    aov = revenue / total_orders if total_orders > 0 else 0.0

    # -------------------------------
    # ✅ REPEAT CUSTOMER RATE
    # % of customers with >1 order
    # -------------------------------

    from sqlalchemy import func

    order_counts = (
        session.query(Order.customer_id, func.count(Order.id).label("cnt"))
        .group_by(Order.customer_id)
        .all()
    )

    if order_counts:
        repeat_customers = sum(1 for c in order_counts if c.cnt > 1)
        repeat_rate = (repeat_customers / len(order_counts)) * 100
    else:
        repeat_rate = 0.0

    # -------------------------------
    # ✅ DAILY ORDERS CHART
    # -------------------------------

    daily_orders = (
        session.query(
            func.date(Order.order_date).label("day"),
            func.count(Order.id).label("orders")
        )
        .group_by(func.date(Order.order_date))
        .order_by(func.date(Order.order_date))
        .all()
    )

    df_daily = pd.DataFrame(daily_orders, columns=["day", "orders"])
    fig_daily = px.line(df_daily, x="day", y="orders", title="Orders per Day")
    fig_daily_json = fig_daily.to_json()

    # -------------------------------
    # ✅ TOP CUSTOMERS CHART
    # -------------------------------

    top_customers = (
        session.query(
            Order.customer_id,
            func.sum(Order.order_amount).label("total_revenue")
        )
        .group_by(Order.customer_id)
        .order_by(func.sum(Order.order_amount).desc())
        .limit(10)
        .all()
    )

    df_top = pd.DataFrame(top_customers, columns=["customer_id", "total_revenue"])
    fig_top = px.bar(df_top, x="customer_id", y="total_revenue", title="Top 10 Customers")
    fig_top_json = fig_top.to_json()

    session.close()

    # -------------------------------
    # ✅ BUILD RETURN OBJECT
    # -------------------------------

    cards = [
        KPICard("Total Customers", total_customers),
        KPICard("Total Orders", total_orders),
        KPICard("Revenue", f"{revenue:,.2f}"),
        KPICard("Average Order Value", f"{aov:,.2f}"),
        KPICard("Repeat Customer Rate", f"{repeat_rate:.1f}%"),
    ]

    charts = [
        KPIChart(id="daily_orders", title="Orders Per Day", fig_json=fig_daily_json),
        KPIChart(id="top_customers", title="Top Customers", fig_json=fig_top_json)
    ]

    return KPIBundle(engine="db", cards=cards, charts=charts)
