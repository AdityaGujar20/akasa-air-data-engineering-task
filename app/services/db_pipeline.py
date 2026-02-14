import os
import pandas as pd
from datetime import datetime
from loguru import logger
from app.db.db_config import get_engine, get_session, Base
from app.db.models import Customer, Order

DATA_DIR = "data/cleaned"
CUSTOMERS_CLEANED = os.path.join(DATA_DIR, "customers_cleaned.csv")
ORDERS_CLEANED = os.path.join(DATA_DIR, "orders_cleaned.csv")

def _ensure_files():
    for path in [CUSTOMERS_CLEANED, ORDERS_CLEANED]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing cleaned file: {path}")

def create_tables():
    logger.info("✅ Creating (or verifying) tables…")
    Base.metadata.create_all(get_engine())
    logger.success("✅ Tables ready.")

def _require_columns(df: pd.DataFrame, cols: list[str], what: str):
    for c in cols:
        if c not in df.columns:
            raise ValueError(f"{what} missing required column: {c}")

def load_customers(session) -> int:
    df = pd.read_csv(CUSTOMERS_CLEANED)
    _require_columns(df, ["customer_id", "customer_name"], "customers_cleaned.csv")

    # idempotent load: truncate & bulk insert
    session.query(Customer).delete()
    objs = [
        Customer(
            customer_id=int(r["customer_id"]),
            customer_name=str(r["customer_name"])
        )
        for _, r in df.iterrows()
    ]
    session.bulk_save_objects(objs)
    return len(objs)

def _parse_date(x: str):
    # robust date parsing (expects ISO or common formats)
    return datetime.fromisoformat(x).date() if "T" in x or "-" in x else datetime.strptime(x, "%Y-%m-%d").date()

def load_orders(session) -> int:
    df = pd.read_csv(ORDERS_CLEANED)
    _require_columns(df, ["order_id", "customer_id", "order_amount", "order_date"], "orders_cleaned.csv")

    # normalize types
    df["order_id"] = df["order_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["order_amount"] = df["order_amount"].astype(float)
    df["order_date"] = df["order_date"].map(_parse_date)

    session.query(Order).delete()
    objs = [
        Order(
            order_id=int(r["order_id"]),
            customer_id=int(r["customer_id"]),
            order_amount=float(r["order_amount"]),
            order_date=r["order_date"],
        )
        for _, r in df.iterrows()
    ]
    session.bulk_save_objects(objs)
    return len(objs)

def run_db_pipeline() -> dict:
    logger.info("🚀 Starting DB pipeline…")
    _ensure_files()
    create_tables()
    with get_session() as s:
        c = load_customers(s)
        o = load_orders(s)
    logger.success(f"🎉 DB pipeline done. Loaded: customers={c}, orders={o}")
    return {"customers_loaded": c, "orders_loaded": o}
