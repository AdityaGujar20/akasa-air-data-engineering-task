# app/services/file_partition.py
import os
import glob
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from loguru import logger

from app.services.cleaning_pipeline import clean_customers, clean_orders

UPLOAD_ROOT = "data/upload"
CLEANED_DIR = "data/cleaned"
CLEANED_CUSTOMERS = os.path.join(CLEANED_DIR, "customers_cleaned.csv")
CLEANED_ORDERS = os.path.join(CLEANED_DIR, "orders_cleaned.csv")

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def get_today_partition_dir() -> str:
    # Use IST date for partition naming
    ist_today = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
    return os.path.join(UPLOAD_ROOT, ist_today)

def _list_partition_dirs():
    ensure_dir(UPLOAD_ROOT)
    # return only directories
    return sorted([d for d in glob.glob(os.path.join(UPLOAD_ROOT, "*")) if os.path.isdir(d)])

def _read_all_customers() -> pd.DataFrame:
    frames = []
    for d in _list_partition_dirs():
        path = os.path.join(d, "customers.csv")
        if os.path.exists(path):
            try:
                frames.append(pd.read_csv(path))
            except Exception as e:
                logger.warning(f"Skipping customers in {d}: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

def _read_all_orders() -> pd.DataFrame:
    frames = []
    for d in _list_partition_dirs():
        path = os.path.join(d, "orders.xml")
        if os.path.exists(path):
            try:
                frames.append(pd.read_xml(path))
            except Exception as e:
                logger.warning(f"Skipping orders in {d}: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

def merge_and_clean_all_batches():
    """
    Reads ALL daily partitions from data/upload/,
    merges them, cleans them with existing cleaning functions,
    and writes data/cleaned/customers_cleaned.csv + orders_cleaned.csv.
    Returns a small summary dict.
    """
    logger.info("🔎 Merging all partitions from data/upload/* ...")
    ensure_dir(CLEANED_DIR)

    raw_customers = _read_all_customers()
    raw_orders = _read_all_orders()

    summary = {
        "partitions": _list_partition_dirs(),
        "raw_customers_rows": int(len(raw_customers)) if not raw_customers.empty else 0,
        "raw_orders_rows": int(len(raw_orders)) if not raw_orders.empty else 0,
        "cleaned_customers_rows": 0,
        "cleaned_orders_rows": 0,
    }

    # Clean + write only if data exists; allow partial uploads
    if not raw_customers.empty:
        cleaned_customers = clean_customers(raw_customers)
        cleaned_customers.to_csv(CLEANED_CUSTOMERS, index=False)
        summary["cleaned_customers_rows"] = int(len(cleaned_customers))
        logger.success(f"✅ Wrote {CLEANED_CUSTOMERS} ({len(cleaned_customers)} rows)")
    else:
        logger.warning("No customers data found across partitions. Skipping customers_cleaned.csv.")

    if not raw_orders.empty:
        cleaned_orders = clean_orders(raw_orders)
        cleaned_orders.to_csv(CLEANED_ORDERS, index=False)
        summary["cleaned_orders_rows"] = int(len(cleaned_orders))
        logger.success(f"✅ Wrote {CLEANED_ORDERS} ({len(cleaned_orders)} rows)")
    else:
        logger.warning("No orders data found across partitions. Skipping orders_cleaned.csv.")

    return summary
