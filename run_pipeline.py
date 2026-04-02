from datetime import datetime, timezone
import sys
import time
from snapshot import run_snapshot
from alert_engine import main as run_alert_engine
from generator import main as generate_data



def run_pipeline(snapshot_date=None):

    if snapshot_date is None:
        snapshot_date = datetime.now(timezone.utc).date()

    print(f"Pipeline started for {snapshot_date}")

    try:
        generate_data()

    except Exception as e:
        print(f"Generator error: {e}")
        sys.exit()

    for i in range(3):
        try:
            run_snapshot(snapshot_date)
            break
        except Exception as e:
            print(f"Snapshot attempt {i+1} failed: {e}")
            time.sleep(2)
    else:
        print(f"System failure.")
        sys.exit()

    for i in range(3):
        try:
            run_alert_engine(snapshot_date)
            break
        except Exception as e:
            print(f"Alert engine attempt {i+1} failed: {e}")
            time.sleep(2)
    else:
        print(f"System failure.")
        sys.exit()
    
    print("See you in another life, brother.")
    