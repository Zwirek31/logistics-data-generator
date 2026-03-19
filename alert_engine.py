from sqlalchemy import create_engine, text
import sys
import json
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def connect_to_db():
    try:
        db_url = os.getenv("SUPABASE_DB_URL")
        engine = create_engine(db_url)
        return engine

    except Exception as e:
        print(f"Startup error: {e}")
        sys.exit()

def load_state(filename):
    if not os.path.exists(filename):
        return None 
    with open(filename, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return None
        cursor = data.get("last_processed_week")
        if isinstance(cursor, str) and cursor.strip():
            try:
                last_processed_week = datetime.fromisoformat(cursor)
                return last_processed_week
            except ValueError:
                return None
    return None

def fetch_weeks(engine, last_processed_week):

    query = "SELECT week_start, revenue_delta, delay_percentage FROM weekly_kpi_with_delta"
    params = {}
    conditions = ["revenue_delta IS NOT NULL"]

    if last_processed_week is not None:
        conditions.append("week_start > :last_processed_week")
        params["last_processed_week"] = last_processed_week

    query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY week_start"

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        unprocessed_weeks = [(row.week_start, row.revenue_delta, row.delay_percentage) for row in result]

    return unprocessed_weeks

def get_last_alert_state(engine):
    alert_types = ["REVENUE_DROP", "DELAY_SPIKE"]
    states = {}

    with engine.connect() as conn:
        for alert_type in alert_types:
            stmt = (text("SELECT event_type FROM alert_log WHERE alert_type= :alert_type ORDER BY created_at DESC LIMIT 1"))
            result = conn.execute(stmt, {"alert_type": alert_type})
            row = result.fetchone()

            if row is None:
                last_event = None
            else:
                last_event = row.event_type
            states[alert_type] = last_event

    return states

def run_alert_engine(engine, unprocessed_weeks, states):

    stmt = (text("""INSERT INTO alert_log (
                        week_start,
                        alert_type,
                        event_type
                    )
            VALUES (:week_start, :alert_type, :event_type)"""))
    
    with engine.begin() as conn:
        for week_start, revenue_delta, delay_percentage in unprocessed_weeks:

            is_drop = revenue_delta <= -0.2
            is_spike = delay_percentage >= 0.2

            state = states["REVENUE_DROP"]

            if is_drop and state != "RAISED":
                conn.execute(stmt, {
                    "week_start": week_start,
                    "alert_type": "REVENUE_DROP",
                    "event_type": "RAISED" 
                })
                states["REVENUE_DROP"]="RAISED"

            elif not is_drop and state == "RAISED":
                conn.execute(stmt, {
                "week_start":week_start,
                "alert_type":"REVENUE_DROP",
                "event_type":"RESOLVED"
                })
                states["REVENUE_DROP"] = "RESOLVED"
            
            state = states["DELAY_SPIKE"]

            if is_spike and state != "RAISED":
                conn.execute(stmt, {
                "week_start": week_start,
                "alert_type": "DELAY_SPIKE",
                "event_type": "RAISED" 
                })
                states["DELAY_SPIKE"] = "RAISED"

            elif not is_spike and state =="RAISED":
                conn.execute(stmt, {
                "week_start": week_start,
                "alert_type": "DELAY_SPIKE",
                "event_type": "RESOLVED" 
                })
                states["DELAY_SPIKE"] = "RESOLVED"

    if unprocessed_weeks:
        new_last_processed_week = unprocessed_weeks[-1][0]
    
    else:
        new_last_processed_week = None

    return new_last_processed_week

def save_state(filename, new_last_processed_week):

    if new_last_processed_week:
        last_processed_week = new_last_processed_week.isoformat()
    else:
        last_processed_week = None

    with open(filename, "w") as f:
        json.dump(last_processed_week, f)

def main():

    filename = "state.json"

    engine = connect_to_db()

    last_processed_week = load_state(filename)

    unprocessed_weeks = fetch_weeks(engine, last_processed_week)

    states = get_last_alert_state(engine)

    new_last_processed_week = run_alert_engine(engine, unprocessed_weeks, states)

    save_state(filename, new_last_processed_week)

if __name__ == "__main__":

    main()




