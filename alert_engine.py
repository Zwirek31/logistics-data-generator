from sqlalchemy import create_engine, text
import sys
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random

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
                last_cursor = datetime.fromisoformat(cursor)
                return last_cursor
            except ValueError:
                return None
    return None

def fetch_weeks(engine, last_processed_week):

    query = "SELECT week_start, revenue_delta, delay_percentage FROM weekly_kpi_with_delta"
    params = {}
    
    with engine.connect() as conn:
        if last_processed_week is not None:
            query += " WHERE week_start > :last_processed_week"
            params["last_processed_week"] = last_processed_week

        query += " ORDER BY week_start"
        stmt = text(query)
        result = conn.execute(stmt, params)
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



def main():
    filename = "state.json"

    engine = connect_to_db()
