import json
import pandas as pd
import plotly.express as px
from sqlalchemy import func, select
from app.db.db_config import get_session
from app.db.models import Customer, Order
from .base_kpis import KPICard, KPIChart, KPIBundle

def compute_kpis_orm() -> KPIBundle:
    # --- scalar KPIs via ORM ---
    with get_session() as s:
        total_customers = s.execute(select(func.count(func.distinct(Customer.customer_id)))).scalar() or 0
        total_orders = s.execute(select(func.count(Order.order_id))).scalar() or 0
        revenue = float(s.execute(select(func.coalesce(func.sum(Order.order_amount), 0.0))).scalar() or 0.0)
        aov = float(s.execute(select(func.coalesce(func.avg(Order.order_amount), 0.0))).scalar() or 0.0)

        # repeat rate: % of customers with >1 orders
        # counts per customer
        q_counts = select(Order.customer_id, func.count().label("n")).group_by(Order.customer_id)
        counts = s.execute(q_counts).all()
        if counts:
            more_than_one = sum(1 for _, n in counts if n > 1)
            repeat_rate = 100.0 * more_than_one / len(counts)
        else:
            repeat_rate = 0.0

        # charts
        # daily orders
        q_daily = select(Order.order_date.label("day"), func.count().label("orders")).group_by(Order.order_date).order_by(Order.order_date)
        daily_rows = s.execute(q_daily).all()
        daily_df = pd.DataFrame(daily_rows, columns=["day", "orders"])
        fig_daily = px.line(daily_df, x="day", y="orders", title="Orders per Day")
        fig_daily_json = fig_daily.to_json()

        # top customers by revenue
        q_top = (
            select(Order.customer_id, func.coalesce(func.sum(Order.order_amount), 0.0).label("revenue"))
            .group_by(Order.customer_id)
            .order_by(func.coalesce(func.sum(Order.order_amount), 0.0).desc())
            .limit(10)
        )
        top_rows = s.execute(q_top).all()
        top_df = pd.DataFrame(top_rows, columns=["customer_id", "revenue"])
        fig_top = px.bar(top_df, x="customer_id", y="revenue", title="Top 10 Customers by Revenue")
        fig_top_json = fig_top.to_json()

    cards = [
        KPICard(title="Total Customers", value=int(total_customers)),
        KPICard(title="Total Orders", value=int(total_orders)),
        KPICard(title="Revenue", value=f"{revenue:,.2f}"),
        KPICard(title="Avg Order Value", value=f"{aov:,.2f}"),
        KPICard(title="Repeat Customer Rate", value=f"{repeat_rate:.1f}%"),
    ]
    charts = [
        KPIChart(id="orders_daily", title="Orders per Day", fig_json=fig_daily_json),
        KPIChart(id="top_customers", title="Top Customers", fig_json=fig_top_json),
    ]
    return KPIBundle(engine="db", cards=cards, charts=charts)
