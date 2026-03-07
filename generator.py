import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os
from dotenv import load_dotenv

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
        RETURNING id
        """)

    with engine.connect() as conn:
        result = conn.execute(stmt, data)

        warehouse_ids = [row[0] for row in result.fetchall()]

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
            RETURNING id
            """)

    with engine.connect() as conn:
        result = conn.execute(stmt, customers)

        customer_ids = [row[0] for row in result.fetchall()]
        
        conn.commit()

    return customer_ids

def main():

    engine = connect_to_db()

    clear_tables(engine)

    warehouse_ids = insert_warehouses(engine)

    customer_ids = insert_customers(engine)


if __name__ == "__main__":

    main()