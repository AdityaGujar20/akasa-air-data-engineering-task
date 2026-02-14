# app/services/spark_pipeline.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, date_format, sum as spark_sum, countDistinct,
    desc, lit, current_date
)
import pyspark.sql.functions as F
from loguru import logger
from datetime import datetime, timedelta


CUSTOMERS_PATH = "data/cleaned/customers_cleaned.csv"
ORDERS_PATH = "data/cleaned/orders_cleaned.csv"


def create_spark_session():
    """Initialize Spark session."""
    logger.info("🔥 Starting Spark Session...")
    spark = (
        SparkSession.builder
        .appName("AkasaAirSparkPipeline")
        .master("local[*]")            # runs locally on all cores
        .getOrCreate()
    )
    logger.success("✅ Spark Session created.")
    return spark


def load_data(spark):
    """Load cleaned CSVs into Spark DataFrames."""
    logger.info("📥 Loading cleaned CSVs into Spark...")

    customers = spark.read.csv(CUSTOMERS_PATH, header=True, inferSchema=True)
    orders = spark.read.csv(ORDERS_PATH, header=True, inferSchema=True)

    # Convert order_date_time to timestamp
    orders = orders.withColumn("order_date_time", col("order_date_time").cast("timestamp"))

    logger.success("✅ Spark DataFrames loaded.")
    return customers, orders


def join_data(customers, orders):
    """Join customers and orders on mobile_number."""
    logger.info("🔗 Joining DataFrames...")

    df = orders.join(customers, on="mobile_number", how="left")

    logger.success(f"✅ Joined Spark DataFrame with {df.count()} rows.")
    return df


def kpi_monthly_order_trend(df):
    """Monthly order count trend."""
    logger.info("📊 Calculating monthly order trends (Spark)...")

    df_month = df.withColumn("year_month", date_format(col("order_date_time"), "yyyy-MM"))

    trend_df = (
        df_month.groupBy("year_month")
        .agg(countDistinct("order_id").alias("order_count"))
        .orderBy("year_month")
    )

    trend = {row["year_month"]: row["order_count"] for row in trend_df.collect()}
    return trend


# kpi_region_revenue (order-level)
def kpi_region_revenue(df):
    # one row per order_id
    w_orders = (
        df.orderBy(F.col("order_date_time").desc())
          .dropDuplicates(["order_id"])
          .select("order_id", "region", "total_amount")
    )
    revenue_df = (
        w_orders.groupBy("region")
        .agg(spark_sum("total_amount").alias("revenue"))
        .orderBy(desc("revenue"))
    )
    return {row["region"]: row["revenue"] for row in revenue_df.collect()}



def kpi_repeat_customers(df):
    """Customers who placed more than one order."""
    logger.info("🔁 Calculating repeat customers (Spark)...")

    repeat_df = (
        df.groupBy("customer_id")
        .agg(countDistinct("order_id").alias("order_count"))
        .filter(col("order_count") > 1)
    )

    repeat_customers = [row["customer_id"] for row in repeat_df.collect()]

    return {
        "count": len(repeat_customers),
        "repeat_customers": repeat_customers
    }


# kpi_top_spenders (order-level in last X days)
def kpi_top_spenders(df, days=30):
    cutoff = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    w_recent_orders = (
        df.filter(col("order_date_time") >= cutoff)
          .orderBy(F.col("order_date_time").desc())
          .dropDuplicates(["order_id"])
          .select("order_id", "customer_id", "customer_name", "total_amount")
    )
    spend_df = (
        w_recent_orders.groupBy("customer_id", "customer_name")
        .agg(spark_sum("total_amount").alias("amount"))
        .orderBy(desc("amount"))
    )
    return [
        {"customer_id": r["customer_id"], "customer_name": r["customer_name"], "amount": r["amount"]}
        for r in spend_df.collect()
    ]



def run_spark_pipeline():
    """Main Spark KPI pipeline."""
    logger.info("🚀 Starting Spark KPI Pipeline...")

    spark = create_spark_session()

    customers, orders = load_data(spark)
    df = join_data(customers, orders)

    kpis = {
        "monthly_order_trend": kpi_monthly_order_trend(df),
        "region_revenue": kpi_region_revenue(df),
        "repeat_customers": kpi_repeat_customers(df),
        "top_spenders_last_30_days": kpi_top_spenders(df),
    }

    logger.success("🎉 Spark KPI pipeline completed!")
    spark.stop()
    return kpis


if __name__ == "__main__":
    results = run_spark_pipeline()
    print(results)
