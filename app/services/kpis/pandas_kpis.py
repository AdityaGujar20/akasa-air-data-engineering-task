# app/services/kpis/pandas_kpis.py
import json
import pandas as pd
import plotly.express as px
from typing import Tuple
from .base_kpis import KPICard, KPIChart, KPIBundle

CUSTOMERS_PATH = "data/cleaned/customers_cleaned.csv"
ORDERS_PATH    = "data/cleaned/orders_cleaned.csv"

def _load_cleaned() -> Tuple[pd.DataFrame, pd.DataFrame]:
    customers = pd.read_csv(CUSTOMERS_PATH)  # expects: customer_id
    orders = pd.read_csv(ORDERS_PATH, parse_dates=["order_date"])  # expects: customer_id, order_amount, order_date
    return customers, orders

def compute_pandas_bundle() -> KPIBundle:
    customers, orders = _load_cleaned()

    # --- Basic KPIs (adjust column names if needed)
    total_customers = customers["customer_id"].nunique()
    total_orders = len(orders)
    revenue = float(orders["order_amount"].sum()) if "order_amount" in orders else 0.0
    aov = float(orders["order_amount"].mean()) if "order_amount" in orders and total_orders else 0.0

    # Repeat customer rate
    orders_per_customer = orders.groupby("customer_id").size()
    repeat_rate = float((orders_per_customer > 1).mean() * 100) if len(orders_per_customer) else 0.0

    # --- Charts
    # Orders over time (daily)
    daily = orders.resample("D", on="order_date").size().rename("orders").reset_index()
    fig_daily = px.line(daily, x="order_date", y="orders", title="Orders per Day")
    fig_daily_json = fig_daily.to_json()

    # Top customers by revenue
    if "order_amount" in orders:
        by_customer = orders.groupby("customer_id")["order_amount"].sum().sort_values(ascending=False).head(10).reset_index()
        fig_top = px.bar(by_customer, x="customer_id", y="order_amount", title="Top 10 Customers by Revenue")
        fig_top_json = fig_top.to_json()
    else:
        fig_top_json = px.bar(pd.DataFrame({"customer_id": [], "order_amount": []}), x="customer_id", y="order_amount",
                              title="Top 10 Customers by Revenue").to_json()

    cards = [
        KPICard(title="Total Customers", value=total_customers),
        KPICard(title="Total Orders", value=total_orders),
        KPICard(title="Revenue", value=f"{revenue:,.2f}"),
        KPICard(title="Avg Order Value", value=f"{aov:,.2f}"),
        KPICard(title="Repeat Customer Rate", value=f"{repeat_rate:.1f}%"),
    ]
    charts = [
        KPIChart(id="orders_daily", title="Orders per Day", fig_json=fig_daily_json),
        KPIChart(id="top_customers", title="Top Customers", fig_json=fig_top_json),
    ]
    return KPIBundle(engine="pandas", cards=cards, charts=charts)
