from db import connect_to_db
from sqlalchemy import text

def run_snapshot(snapshot_date):
    engine = connect_to_db()

    with engine.begin() as conn:
        query = text("""
            INSERT INTO weekly_kpi_snapshot 
            (snapshot_date, week_start, shipment_count, weekly_revenue, delay_percentage)
            SELECT
                :snapshot_date::DATE,
                week_start,
                shipment_count,
                weekly_revenue,
                delay_percentage
            FROM weekly_kpi
            WHERE week_start < date_trunc('week', :snapshot_date::DATE)
            ON CONFLICT (snapshot_date, week_start)
            DO NOTHING
        """)
    
        params = {"snapshot_date": snapshot_date}

        conn.execute(query, params)
