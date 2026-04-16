from datetime import timedelta
import random

def generate_shipments_for_n_weeks(today, warehouse_ids, customer_ids, n=10):
    
    monday = today - timedelta(days=today.weekday())
    week_start = []
    
    for i in range(n):
        start_of_week = monday + timedelta(days=i*7)
        week_start.append(start_of_week)

    shipments_to_simulate = []
    
    for week in week_start:
        number_of_shipments = random.randint(15, 20)

        for _ in range(number_of_shipments):
            warehouse_id = random.choice(warehouse_ids)
            customer_id = random.choice(customer_ids)
            day_offset = random.randint(0, 6)
            hour_offset = random.randint(0, 23)
            created_at = week + timedelta(days=day_offset, hours=hour_offset)
            random_hours = random.randint(24, 72)
            planned_delivery_date = created_at + timedelta(hours=random_hours)
            prep_time = random.randint(12, 48)
            ready_at = created_at + timedelta(hours=prep_time)
            status = None
            
            shipment = {
            "customer_id": customer_id,
            "created_at": created_at,
            "warehouse_id": warehouse_id,
            "planned_delivery_date": planned_delivery_date,
            "actual_delivery_date": None,
            "ready_at": ready_at,
            "status": status
        }
        
            shipments_to_simulate.append(shipment)
            
    return shipments_to_simulate