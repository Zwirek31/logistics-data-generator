from datetime import datetime, timezone, date, timedelta
import sys
import time
from generate_raw_shipments import generate_shipments_for_day
from simulate_shipments import SimulationEngine
from snapshot import run_snapshot
from alert_engine import main as run_alert_engine
from generator import main as generate_data
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

load_dotenv()

def run_simulation(start_date, end_date, demand_pattern):
    days_to_simulate = (end_date - start_date).days + 1
    date_range = [start_date + timedelta(days=i) for i in range(days_to_simulate)]

    engine = SimulationEngine()

    logging.info("Simulation started for %s - %s", start_date, end_date)

    for i, day in enumerate(date_range):
        demand = demand_pattern[i % len(demand_pattern)]
        logging.info("Backlog before: %s", len(engine.queue))
        new_shipments = generate_shipments_for_day(day, demand)
        logging.info("Incoming: %s", len(new_shipments))
        finished = engine.process_day(day, new_shipments)
        logging.info("Processed: %s", len(finished))
        insert_shipments(finished)
        logging.info("Backlog after: %s", len(engine.queue))

def run_analytics(snapshot_date=None):

    if snapshot_date is None:
        snapshot_date = datetime.now(timezone.utc).date()
        
    logging.info("Analitycs started for %s", snapshot_date)

    try:
        generate_data()
    except Exception as e:
        logging.error(f"Generator error: {e}")
        sys.exit()

    for i in range(3):
        try:
            run_snapshot(snapshot_date)
            break
        except Exception as e:
            logging.warning(f"Snapshot attempt {i+1} failed: {e}")
            time.sleep(2)
    else:
        logging.error(f"System failure.")
        sys.exit()

    for i in range(3):
        try:
            run_alert_engine(snapshot_date)
            break
        except Exception as e:
            logging.warning(f"Alert engine attempt {i+1} failed: {e}")
            time.sleep(2)
    else:
        logging.error(f"System failure.")
        sys.exit()
    
    logging.info("See you in another life, brother.")

    def main():
        start_date = date(2026, 4, 1)
        end_date = date(2026, 4, 30)
        demand_pattern= [20,20,20,35,35,35,20,20]
        run_simulation(start_date, end_date, demand_pattern)

    