import pandas as pd
import os
from datetime import datetime
from loguru import logger

# Paths – these directories ALREADY exist in your repo
UPLOAD_DIR = "data/upload"
CLEANED_DIR = "data/cleaned"

RAW_CUSTOMER_PATH = os.path.join(UPLOAD_DIR, "customers.csv")
RAW_ORDER_PATH = os.path.join(UPLOAD_DIR, "orders.xml")

CLEANED_CUSTOMER_PATH = os.path.join(CLEANED_DIR, "customers_cleaned.csv")
CLEANED_ORDER_PATH = os.path.join(CLEANED_DIR, "orders_cleaned.csv")


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning customer data...")

    # Drop rows with missing essential fields
    df = df.dropna(subset=["customer_id", "mobile_number"])

    # Strip whitespace and standardize casing
    df["customer_name"] = df["customer_name"].astype(str).str.strip().str.title()
    df["region"] = df["region"].astype(str).str.strip().str.title()

    # Clean mobile numbers (keep only digits)
    df["mobile_number"] = (
        df["mobile_number"].astype(str).str.replace(r"\D", "", regex=True)
    )

    # Deduplicate
    df = df.drop_duplicates(subset=["customer_id", "mobile_number"])

    logger.info(f"✅ Customers cleaned → {len(df)} rows")
    return df



def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean raw order rows (SKU-level) to a normalized frame."""
    logger.info("Cleaning order data...")

    df = df.dropna(subset=["order_id", "mobile_number", "order_date_time"])

    # Clean mobile numbers
    df["mobile_number"] = df["mobile_number"].astype(str).str.replace(r"\D", "", regex=True)

    # Convert date field
    df["order_date_time"] = pd.to_datetime(df["order_date_time"], errors="coerce")
    df = df.dropna(subset=["order_date_time"])

    # Numeric conversions
    if "total_amount" in df.columns:
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0).astype(float)
    elif "order_amount" in df.columns:
        # fallback if XML already provides order_amount
        df.rename(columns={"order_amount": "total_amount"}, inplace=True)
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0).astype(float)
    else:
        df["total_amount"] = 0.0

    # Remove duplicates on order_id + sku_id if present
    subset_cols = [c for c in ["order_id", "sku_id"] if c in df.columns]
    if subset_cols:
        df = df.drop_duplicates(subset=subset_cols)

    logger.info(f"✅ Orders cleaned (row-level) → {len(df)} rows")
    return df


def run_cleaning_pipeline():
    """Main cleaning pipeline that outputs order-level data for DB pipeline."""
    logger.info("🚀 Starting cleaning pipeline...")

    try:
        customers_df = pd.read_csv(RAW_CUSTOMER_PATH)
        logger.info(f"Loaded customers.csv → {len(customers_df)} rows")
    except Exception as e:
        logger.error(f"Error loading customers.csv: {e}")
        return

    try:
        orders_df = pd.read_xml(RAW_ORDER_PATH)
        logger.info(f"Loaded orders.xml → {len(orders_df)} rows")
    except Exception as e:
        logger.error(f"Error loading orders.xml: {e}")
        return

    cleaned_customers = clean_customers(customers_df)
    cleaned_orders_row_level = clean_orders(orders_df)

    # Map mobile_number -> customer_id (and name if needed)
    cust_map = cleaned_customers[["customer_id", "mobile_number"]].copy()
    cust_map["mobile_number"] = cust_map["mobile_number"].astype(str).str.replace(r"\D", "", regex=True)

    orders_with_customer = cleaned_orders_row_level.merge(
        cust_map, on="mobile_number", how="left"
    )

    # Aggregate to ORDER-LEVEL as required by DB pipeline
    order_level = (
        orders_with_customer
        .groupby(["order_id", "customer_id"], dropna=False)
        .agg(
            order_amount=("total_amount", "sum"),
            order_date=("order_date_time", "max"),
        )
        .reset_index()
    )

    # Ensure proper types and date-only column
    order_level["order_amount"] = order_level["order_amount"].astype(float)
    order_level["order_date"] = pd.to_datetime(order_level["order_date"]).dt.date
    order_level = order_level[["order_id", "customer_id", "order_amount", "order_date"]]

    # Persist outputs
    os.makedirs(CLEANED_DIR, exist_ok=True)
    cleaned_customers.to_csv(CLEANED_CUSTOMER_PATH, index=False)
    order_level.to_csv(CLEANED_ORDER_PATH, index=False)

    logger.success("🎉 Cleaning pipeline complete!")
    logger.success(f"Saved cleaned customers → {CLEANED_CUSTOMER_PATH}")
    logger.success(f"Saved cleaned orders →    {CLEANED_ORDER_PATH}")


if __name__ == "__main__":
    run_cleaning_pipeline()
