import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random

load_dotenv()

def connect_to_db():
    try:
        db_url = os.getenv("SUPABASE_DB_URL")
        engine = create_engine(db_url)
        return engine

    except Exception as e:
        print(f"Startup error: {e}")
        sys.exit()

def clear_tables(engine):
    tables = ["invoices", "shipments", "customers", "warehouses"]

    with engine.connect() as conn:
        table_list = ", ".join(tables)

        conn.execute(text(f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE;"))

        conn.commit()

def insert_warehouses(engine):

    data = [
        {"name": "Warsaw Hub", "address": "Logistics Street 1", "city": "Warsaw"},
        {"name": "Berlin Hub", "address": "Industriestrasse 4", "city": "Berlin"},
        {"name": "Prague Hub", "address": "Cargo Road 12","city": "Prague"},
        {"name": "Vienna Hub", "address": "Transportplatz 2", "city": "Vienna"},
        {"name": "Budapest Hub", "address": "Depot 8", "city": "Budapest"}
        ]

    stmt = text("""INSERT INTO warehouses(name, address, city)
        VALUES (:name, :address, :city)
        RETURNING warehouse_id
        """)

    with engine.connect() as conn:
        warehouse_ids = []
        
        for warehouse in data: 
            result = conn.execute(stmt, warehouse)
            warehouse_ids.append(result.scalar())

        conn.commit()

        return warehouse_ids

def insert_customers(engine):
    customers = [
        {"name": "TechCore Solutions", "industry": "Technology"},
        {"name": "GreenLeaf Logistics", "industry": "Transportation"},
        {"name": "Global Retail Group", "industry": "Retail"},
        {"name": "Alpha Manufacturing", "industry": "Manufacturing"},
        {"name": "Stellar Health Systems", "industry": "Healthcare"},
        {"name": "Everest Financial", "industry": "Finance"},
        {"name": "EcoBuild Construction", "industry": "Construction"},
        {"name": "BlueWave Media", "industry": "Marketing"}
        ]

    stmt = text("""INSERT INTO customers(name, industry)
            VALUES (:name, :industry)
            RETURNING customer_id
            """)

    with engine.connect() as conn:
        customer_ids = []

        for customer in customers:
            result = conn.execute(stmt, customer)
            customer_ids.append(result.scalar())

        conn.commit()

        return customer_ids

def generate_shipments_for_5_weeks(base_date, warehouse_ids, customer_ids):
    
    week_start = []
    
    for week in range(5):
        shift = week * 7
        week_number = base_date + timedelta(days=shift)
        week_start.append(week_number)

    shipment_list = []

    for week in week_start:
        number_of_shipments = random.randint(8, 15)
        
        for _ in range(number_of_shipments):
            warehouse_id = random.choice(warehouse_ids)
            customer_id = random.choice(customer_ids)
            hours = random.randint(0, 72)
            created_at = week + timedelta(hours=hours)
            random_hours = random.randint(6, 24)
            planned_delivery_date = created_at + timedelta(hours=random_hours)
            delay = random.randint(0, 10)
            actual_delivery_date = planned_delivery_date + timedelta(hours=delay)
            status = "DELIVERED"
            shipment = {
                "warehouse_id": warehouse_id,
                "customer_id": customer_id,
                "created_at": created_at,
                "planned_delivery_date": planned_delivery_date,
                "actual_delivery_date": actual_delivery_date,
                "status": status
            }
            shipment_list.append(shipment)
            
    return week_start, shipment_list

def main():

    base_date = datetime(2026, 3, 9)

    engine = connect_to_db()

    clear_tables(engine)

    warehouse_ids = insert_warehouses(engine)

    customer_ids = insert_customers(engine)

if __name__ == "__main__":

    main()