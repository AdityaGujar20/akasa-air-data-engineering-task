# app/services/kpis/spark_kpis.py
import json
from typing import Tuple
from .base_kpis import KPICard, KPIChart, KPIBundle

def compute_spark_bundle() -> KPIBundle:
    try:
        from pyspark.sql import SparkSession, functions as F
    except Exception:
        # Fallback: clearly tell UI Spark isn't available
        cards = [KPICard(title="Spark Status", value="PySpark not installed")]
        return KPIBundle(engine="spark", cards=cards, charts=[])

    spark = SparkSession.builder.appName("AkasaKPIs").getOrCreate()
    customers = spark.read.option("header", True).csv("data/cleaned/customers_cleaned.csv")
    orders = spark.read.option("header", True).option("inferSchema", True).csv("data/cleaned/orders_cleaned.csv")

    # Cast columns (adjust if your column names differ)
    from pyspark.sql.types import DoubleType, TimestampType
    orders = (orders
              .withColumn("order_amount", orders["order_amount"].cast(DoubleType()))
              .withColumn("order_date", orders["order_date"].cast(TimestampType())))

    total_customers = customers.select("customer_id").distinct().count()
    total_orders = orders.count()
    revenue_row = orders.select(F.sum("order_amount")).first()
    revenue = float(revenue_row[0] or 0.0)
    aov = float(revenue / total_orders) if total_orders else 0.0

    oc = orders.groupBy("customer_id").count()
    repeat_rate = float(oc.filter("count > 1").count() / oc.count() * 100) if oc.count() else 0.0

    daily = orders.groupBy(F.to_date("order_date").alias("day")).count().toPandas()
    import plotly.express as px
    fig_daily = px.line(daily, x="day", y="count", title="Orders per Day")
    fig_daily_json = fig_daily.to_json()

    top = orders.groupBy("customer_id").agg(F.sum("order_amount").alias("revenue")).orderBy(F.desc("revenue")).limit(10).toPandas()
    fig_top = px.bar(top, x="customer_id", y="revenue", title="Top 10 Customers by Revenue")
    fig_top_json = fig_top.to_json()

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
    return KPIBundle(engine="spark", cards=cards, charts=charts)
