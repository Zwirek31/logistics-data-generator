from sqlalchemy import text
from datetime import datetime, timedelta
import random
from collections import defaultdict
from math import ceil
from db import connect_to_db

def clear_tables(engine):
    tables = ["invoices", "shipments", "customers", "warehouses"]

    with engine.begin() as conn:
        table_list = ", ".join(tables)
        conn.execute(text(f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE;"))

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

    with engine.begin() as conn:
        warehouse_ids = []
        
        for warehouse in data: 
            result = conn.execute(stmt, warehouse)
            warehouse_ids.append(result.scalar())

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

    with engine.begin() as conn:
        customer_ids = []

        for customer in customers:
            result = conn.execute(stmt, customer)
            customer_ids.append(result.scalar())

    return customer_ids

def generate_shipments_for_5_weeks(base_date, warehouse_ids, customer_ids):
    
    week_start = []
    
    for week in range(5):
        shift = week * 7
        week_number = base_date + timedelta(days=shift)
        week_start.append(week_number)

    shipment_list = []
    spike_happened = False

    for week in week_start:
        if week == week_start[-1] and spike_happened == False:
            is_spike = True
        
        else:
            is_spike = random.random() < 0.2

        number_of_shipments = random.randint(8, 15)

        if is_spike:
            delay_percentage = random.uniform(0.25, 0.40)
            minimal_required = ceil(number_of_shipments * 0.25)
            delayed_count = max(int(number_of_shipments * delay_percentage), minimal_required)
            spike_happened = True
        
        else:
            delay_percentage = random.uniform(0.05, 0.10)
            delayed_count = int(number_of_shipments * delay_percentage)

        on_time_shipments = number_of_shipments - delayed_count

        types = ["delayed"] * delayed_count + ["on_time"] * on_time_shipments

        random.shuffle(types)
        
        for shipment_type in types:
            warehouse_id = random.choice(warehouse_ids)
            customer_id = random.choice(customer_ids)
            hours = random.randint(0, 72)
            created_at = week + timedelta(hours=hours)
            random_hours = random.randint(24, 48)
            planned_delivery_date = created_at + timedelta(hours=random_hours)
            delay = random.randint(1, 12)
            random_seconds = random.randint(0, 6 * 3600)

            if shipment_type == "delayed":
                actual_delivery_date = planned_delivery_date + timedelta(hours=delay)
            
            else:
                actual_delivery_date = planned_delivery_date - timedelta(seconds=random_seconds)

            status = "DELIVERED"
            shipment = {
                "customer_id": customer_id,
                "created_at": created_at,
                "warehouse_id": warehouse_id,
                "planned_delivery_date": planned_delivery_date,
                "actual_delivery_date": actual_delivery_date,
                "status": status
            }
            shipment_list.append(shipment)
            
    return shipment_list

def insert_shipments(engine, shipment_list):

    try:

        stmt = text("""INSERT INTO shipments(customer_id, created_at, warehouse_id, planned_delivery_date, actual_delivery_date, status)
            VALUES (:customer_id, :created_at, :warehouse_id, :planned_delivery_date, :actual_delivery_date, :status)
            """)
    
        with engine.begin() as conn:
            for shipment in shipment_list:
                conn.execute(stmt, shipment)
        
    except Exception as e: 
        print(f"Wystąpił błąd: {e}")


def read_shipments_from_db(engine):
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT shipment_id, DATE_TRUNC('week', actual_delivery_date) as week_start FROM shipments"))
        shipments_rows = [(row.shipment_id, row.week_start) for row in result]

    return shipments_rows


def build_week_shipment_map(shipments_rows):

    week_map = defaultdict(list)

    for shipment_id, week_start in shipments_rows:
        week_map[week_start].append(shipment_id)

    return week_map


def generate_invoices_for_shipments(week_map):

    week_map_sorted = dict(sorted(week_map.items()))
    week_revenue_targets = [ 
        10000,
        9500,
        7000,
        7200,
        9800
        ]
    
    invoices = []

    for (_, shipments_ids), target in zip(week_map_sorted.items(), week_revenue_targets):
        n = len(shipments_ids)
        weights = [random.uniform(0.9, 1.1) for _ in range(n)]
        total_weight = sum(weights)
       
        unit_value = target / total_weight
        
        for (weight, shipment_id) in zip(weights, shipments_ids):
            amount = weight * unit_value
            has_correction = random.random() < 0.15
            if has_correction:
                correction_value = amount * random.uniform(0.05, 0.2)
                normal_amount = amount + correction_value
                correction_amount = -correction_value
                
                invoices.append((shipment_id, normal_amount, "NORMAL", None)) 
                invoices.append((shipment_id, correction_amount, "CORRECTION", shipment_id))
            
            else:
                invoices.append((shipment_id, amount, "NORMAL", None))

    return invoices 
        

def insert_invoices(engine, invoices):

    invoices = [
    dict(
        shipment_id=shipment_id,
        amount=amount,
        invoice_type=invoice_type,
        parent_invoice_id=parent_invoice_id
    )
    for shipment_id, amount, invoice_type, parent_invoice_id in invoices
    ]
    
    normal_invoices = [i for i in invoices if i["invoice_type"] == "NORMAL"]
    correction_invoices = [i for i in invoices if i["invoice_type"] == "CORRECTION"]

    normal_invoice_map = {}

    try:
        stmt = text("""INSERT INTO invoices(shipment_id, amount, invoice_type, parent_invoice_id)
            VALUES (:shipment_id, :amount, :invoice_type, :parent_invoice_id)
                    RETURNING invoice_id
            """)
        
        with engine.begin() as conn:
            for invoice in normal_invoices:
                result = conn.execute(stmt, invoice)
                invoice_id = result.scalar()
                normal_invoice_map[invoice["shipment_id"]] = invoice_id
            
            for invoice in correction_invoices:
                invoice["parent_invoice_id"] = normal_invoice_map[invoice["shipment_id"]]
                conn.execute(stmt, invoice)
                

    except Exception as e: 
        print(f"Wystąpił błąd: {e}")

    
def main():

    base_date = datetime(2026, 3, 9)

    engine = connect_to_db()

    clear_tables(engine)

    warehouse_ids = insert_warehouses(engine)

    customer_ids = insert_customers(engine)

    shipment_list = generate_shipments_for_5_weeks(base_date, warehouse_ids, customer_ids)

    insert_shipments(engine, shipment_list)

    shipment_rows = read_shipments_from_db(engine)

    week_map = build_week_shipment_map(shipment_rows)

    invoices = generate_invoices_for_shipments(week_map)

    insert_invoices(engine, invoices)

if __name__ == "__main__":

    main()