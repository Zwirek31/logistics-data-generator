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
        unprocessed_weeks = result.mappings().all()

    return unprocessed_weeks

def get_last_alert_state(engine):
    
    states = {}

    with engine.connect() as conn:
        query = (text("SELECT alert_type FROM alert_rules"))
        result = conn.execute(query).mappings()
        alert_types = [row["alert_type"] for row in result]

        stmt = text("""
            WITH sorted_events AS (
                SELECT 
                    alert_type, 
                    event_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY alert_type 
                        ORDER BY created_at DESC
                    ) AS rn
                FROM alert_log
            )
            SELECT
            alert_type,
            event_type
            FROM sorted_events
            WHERE rn = 1
        """)

        rows = conn.execute(stmt).fetchall()
        
        states = {row.alert_type: row.event_type for row in rows}
        for alert_type in alert_types:
            if alert_type not in states:
                states[alert_type] = None

    return states

def run_alert_engine(engine, unprocessed_weeks, states):

    stmt = (text("""INSERT INTO alert_log (
                        week_start,
                        alert_type,
                        event_type
                    )
            VALUES (:week_start, :alert_type, :event_type)"""))
    
    with engine.begin() as conn:
        query = (text("""SELECT 
                      alert_type,
                      metric,
                      operator,
                      threshold,
                      recovery_threshold
                        FROM alert_rules"""))
        result = conn.execute(query)
        rules = result.mappings().all()

        def less_equal(a, b):
            return a <= b
            
        def greater_equal(a, b):
            return a >= b
        
        operators = {
            "<=": less_equal,
            ">=": greater_equal
        }

        recovery_ops = {
            "<=": greater_equal, 
            ">=": less_equal
        }

        for row in unprocessed_weeks:
            for rule in rules:
                alert_type = rule["alert_type"]
                state = states[alert_type]
                metric = rule["metric"]
                operator = rule["operator"]
                threshold = rule["threshold"]
                recovery_threshold = rule["recovery_threshold"]
            
                value = row[metric]

                op_trigger = operators[operator]
                op_recovery = recovery_ops[operator]

                is_triggered = op_trigger(value, threshold)
                is_recovered = op_recovery(value, recovery_threshold)

                if is_triggered and state != "RAISED":
                    conn.execute(stmt, {
                    "week_start": row["week_start"],
                    "alert_type": alert_type,
                    "event_type": "RAISED"
                    })
                    states[alert_type] = "RAISED"

                elif is_recovered and state == "RAISED":
                    conn.execute(stmt, {
                    "week_start":row["week_start"],
                    "alert_type":alert_type,
                    "event_type":"RESOLVED"
                    })
                    states[alert_type] = "RESOLVED"

    if unprocessed_weeks:
        new_last_processed_week = unprocessed_weeks[-1]["week_start"]
    
    else:
        new_last_processed_week = None

    return new_last_processed_week

def save_state(filename, new_last_processed_week):

    if new_last_processed_week:
        last_processed_week = new_last_processed_week.isoformat()
    else:
        last_processed_week = None

    data = {
        "last_processed_week": last_processed_week
    }
    with open(filename, "w") as f:
        json.dump(data, f)

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




