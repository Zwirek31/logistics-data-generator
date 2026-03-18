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




def main():
     filename = "state.json"
