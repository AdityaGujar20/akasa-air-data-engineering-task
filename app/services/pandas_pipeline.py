# app/services/pandas_pipeline.py

import pandas as pd
from datetime import datetime, timedelta
from loguru import logger

CUSTOMERS_PATH = "data/cleaned/customers_cleaned.csv"
ORDERS_PATH = "data/cleaned/orders_cleaned.csv"


def load_data():
    """Load cleaned customers and orders."""
    logger.info("📥 Loading cleaned data...")

    customers = pd.read_csv(CUSTOMERS_PATH)
    orders = pd.read_csv(ORDERS_PATH)

    # Ensure datetime conversion
    orders["order_date_time"] = pd.to_datetime(orders["order_date_time"])

    logger.success("✅ Cleaned data loaded successfully.")
    return customers, orders


def join_data(customers, orders):
    """Join customers and orders on mobile_number."""
    logger.info("🔗 Joining customers and orders...")

    df = orders.merge(
        customers,
        on="mobile_number",
        how="left",
        suffixes=("_order", "_customer")
    )

    logger.success(f"✅ Joined dataset → {len(df)} rows")
    return df


def kpi_monthly_order_trend(df):
    """Monthly order count trend."""
    logger.info("📊 Calculating monthly order trends...")

    df["year_month"] = df["order_date_time"].dt.to_period("M").astype(str)
    trend = df.groupby("year_month")["order_id"].nunique().to_dict()

    return trend


# kpi_region_revenue (order-level)
def kpi_region_revenue(df):
    # collapse to distinct orders first
    order_level = (
        df.sort_values("order_date_time")
          .drop_duplicates(subset=["order_id"])  # keep one row per order
          [["order_id", "total_amount", "region"]]
    )
    revenue = order_level.groupby("region")["total_amount"].sum().to_dict()
    return revenue

# kpi_top_spenders (order-level in last 30 days)
from datetime import datetime, timedelta


def kpi_repeat_customers(df):
    """Customers who placed more than one order."""
    logger.info("🔁 Identifying repeat customers...")

    repeat_counts = df.groupby("customer_id")["order_id"].nunique()
    repeat_customers = repeat_counts[repeat_counts > 1].index.tolist()

    return {
        "count": len(repeat_customers),
        "repeat_customers": repeat_customers
    }


def kpi_top_spenders(df, days=30):
    cutoff = datetime.today() - timedelta(days=days)
    order_level = (
        df[df["order_date_time"] >= cutoff]
          .sort_values("order_date_time")
          .drop_duplicates(subset=["order_id"])
          [["order_id", "customer_id", "customer_name", "total_amount"]]
    )
    spenders = (
        order_level.groupby(["customer_id", "customer_name"])["total_amount"]
        .sum()
        .reset_index()
        .sort_values(by="total_amount", ascending=False)
    )
    # keep a consistent key name
    return spenders.rename(columns={"total_amount": "amount"}).to_dict(orient="records")


def run_pandas_pipeline():
    """Main function to compute all KPIs using Pandas."""
    logger.info("🚀 Starting Pandas KPI Pipeline...")

    # Load + Join
    customers, orders = load_data()
    df = join_data(customers, orders)

    # KPIs
    kpis = {
        "monthly_order_trend": kpi_monthly_order_trend(df),
        "region_revenue": kpi_region_revenue(df),
        "repeat_customers": kpi_repeat_customers(df),
        "top_spenders_last_30_days": kpi_top_spenders(df),
    }

    logger.success("🎉 Pandas KPI pipeline completed!")
    return kpis


if __name__ == "__main__":
    results = run_pandas_pipeline()
    print(results)
