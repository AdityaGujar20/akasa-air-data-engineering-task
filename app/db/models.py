from sqlalchemy import Column, Integer, String, Float, Date, Index
from app.db.db_config import Base

class Customer(Base):
    __tablename__ = "customers"
    customer_id = Column(Integer, primary_key=True, index=True)
    customer_name = Column(String(255), nullable=False)

    __table_args__ = (
        Index("ix_customers_customer_name", "customer_name"),
    )

class Order(Base):
    __tablename__ = "orders"
    order_id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, index=True, nullable=False)
    order_amount = Column(Float, nullable=False)
    order_date = Column(Date, nullable=False)

    __table_args__ = (
        Index("ix_orders_customer_id", "customer_id"),
        Index("ix_orders_order_date", "order_date"),
    )
