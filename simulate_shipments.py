from datetime import datetime, timedelta, time
from collections import defaultdict
import random

def run_logistics_simulation(shipments_to_simulate):

    shipments_to_insert = []
    shipments_by_day = defaultdict(list)

    for shipment in shipments_to_simulate:
        day = shipment["created_at"].date()
        shipments_by_day[day].append(shipment)
    
    queue = []
    capacity = 10
    start_day = min(shipments_by_day)
    end_day = max(shipments_by_day)
    current_day = start_day

    while current_day <= end_day or queue:
        if current_day in shipments_by_day:
            for s in shipments_by_day[current_day]:
                queue.append(s)
        
        queue.sort(key=lambda x: x['planned_delivery_date'])
        
        current_time = datetime.combine(current_day, time(8, 0))
        to_be_shipped = []

        for shipment in queue:
            if len(to_be_shipped) == capacity:
                break
            if shipment["ready_at"] <= current_time:
                to_be_shipped.append(shipment)
            
        
        time_per_shipment = 0.75
        
        for i,  shipment in enumerate(to_be_shipped):
            potential_delivery_time = current_time + timedelta(hours=i * time_per_shipment + random.uniform(-0.05, 0.05))
            shipment['status'] = "DELIVERED"
            shipment['actual_delivery_date'] = max(potential_delivery_time, current_time)
            shipments_to_insert.append(shipment)

        queue = [s for s in queue if s not in to_be_shipped]
        current_day += timedelta(days=1)
    
    return shipments_to_insert