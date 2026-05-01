from operations import insert_warehouses
from operations import insert_customers
from db import connect_to_db

def run_reference_data(engine):
    insert_warehouses(engine)
    insert_customers(engine)

def main():
    engine = connect_to_db()
    run_reference_data(engine)

