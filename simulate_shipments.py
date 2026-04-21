from datetime import datetime, timedelta, time
import random

class SimulationEngine():
    def __init__(self, capacity=20):
        self.capacity = capacity
        self.queue = []
    
    def start_of_day(self, day):
        return datetime.combine(day, time(8, 0))

    def process_day(self, day, new_shipments):
        self.queue.extend(new_shipments)
        self.queue.sort(key=lambda x: x['planned_delivery_date'])
        new_queue = []
        to_be_shipped = []
        current_time = self.start_of_day(day)

        for shipment in self.queue:
            if shipment["ready_at"] > current_time:
                new_queue.append(shipment)
            else:
                if len(to_be_shipped) >= self.capacity:
                    new_queue.append(shipment)
                else:    
                    to_be_shipped.append(shipment)
        
        finished_shipemnts = []
        current_time_tracker = current_time

        for shipment in to_be_shipped:
            time_per_shipemnt = timedelta(minutes=random.randint(20, 45))
            delivery_time = current_time_tracker + time_per_shipemnt

            shipment['status'] = "DELIVERED"
            shipment['actual_delivery_date'] = delivery_time

            current_time_tracker = delivery_time
            finished_shipemnts.append(shipment)

        self.queue = new_queue
        
        return finished_shipemnts
    
    