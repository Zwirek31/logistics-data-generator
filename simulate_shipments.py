from datetime import datetime, timedelta, time
from collections import defaultdict
import random

def simulate_shipments(shipment_list):

    shipments_by_day = defaultdict(list)

    for shipment in shipment_list:
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
                s["processing_time"] = random.randint(12, 48)
                queue.append(s)
        
        available_today = []
        for shipment in queue:
            if (shipment["created_at"] + timedelta(hours=shipment["processing_time"])).date() <= current_day:
                available_today.append(shipment)

        available_today.sort(key=lambda x: x['planned_delivery_date'])
        processed_today = min(len(available_today), capacity)
        to_be_shipped = available_today[:processed_today]

        base_offset = 8
        current_time = datetime.combine(current_day, time.min) + timedelta(hours=base_offset)
        time_per_shipment = 0.75
        
        for i,  shipment in enumerate(to_be_shipped):
                random_offset = timedelta(hours=i*time_per_shipment + random.uniform(0, 0.2))
                shipment['status'] = "DELIVERED"
                shipment['actual_delivery_date'] = current_time + random_offset

        queue = [s for s in queue if s not in to_be_shipped]
        current_day += timedelta(days=1)
    