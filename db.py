from sqlalchemy import create_engine
import sys
import os

def connect_to_db():
    try:
        db_url = os.getenv("SUPABASE_DB_URL")
        engine = create_engine(db_url)
        return engine

    except Exception as e:
        print(f"Startup error: {e}")
        sys.exit()