from datetime import timedelta, datetime, time
import random

def generate_shipments_for_day(day, demand, warehouse_ids, customer_ids):
    
    new_shipments = []

    for _ in range(demand):
        warehouse_id = random.choice(warehouse_ids)
        customer_id = random.choice(customer_ids)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        created_at = datetime.combine(day, time(hour, minute))
        prep_time = random.randint(12, 48)
        buffer = random.randint(6, 36)
        ready_at = created_at + timedelta(hours=prep_time)
        planned_delivery_date = ready_at + timedelta(hours=buffer)
        status = None
            
        shipment = {
        "customer_id": customer_id,
        "created_at": created_at,
        "warehouse_id": warehouse_id,
        "ready_at": ready_at,
        "planned_delivery_date": planned_delivery_date,
        "actual_delivery_date": None,
        "status": status
        }
        
        new_shipments.append(shipment)
            
    return new_shipments