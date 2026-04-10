from datetime import datetime, timedelta
from collections import defaultdict
from queue import PriorityQueue
import random

def simulate_shipments(shipment_list):

    shipments_by_day = defaultdict(list)

    for shipment in shipment_list:
        day = shipment["created_at"].date()
        shipments_by_day[day].append(shipment)
    
    queue = []
    available_today = PriorityQueue()
    capacity = 10
    start_day = min(shipments_by_day)
    current_day = start_day

    while shipments_by_day or queue:
        if current_day in shipments_by_day:
            for s in shipments_by_day[current_day]:
                s["processing_time"] = random.randint(12, 48)
                queue.append(s)
        
        for shipment in queue:
            if shipment["created_at"] + timedelta(hours=shipment{"processing_time"}) <= current_day:
                available_today.put((s['planned_delivery_date', s]))
                processed_today = min(len(available_today, capacity))
                
    