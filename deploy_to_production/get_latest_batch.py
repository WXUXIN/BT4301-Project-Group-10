import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import json

USER, PASSWORD, HOST, DB = "bt4301", "password", "localhost", "customer_churn"
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}")


def get_latest():
    query = """
    SELECT 
        t.customer_id,
        t.customer_satisfaction, t.num_complaints, t.num_service_calls, 
        t.late_payments, t.has_phone_service, t.has_internet_service, 
        t.has_tech_support, t.has_streaming_tv, t.has_streaming_movies, 
        t.high_risk_flag, t.is_auto_pay, t.tenure_segment, t.contract
    FROM train_churn_model t
    JOIN dim_customer c ON t.customer_id = c.customer_id
    WHERE c.signup_date >= (
        SELECT DATE_SUB(MAX(signup_date), INTERVAL 2 DAY) 
        FROM dim_customer
    )
    """

    df = pd.read_sql(query, engine)
    df = df.fillna({"contract": "month_to_month", "customer_satisfaction": 0.0}).fillna(
        0
    )

    records = df.to_dict(orient="records")

    filename = "latest_predictions_input.json"
    with open(filename, "w") as f:
        json.dump(records, f, indent=4)

    print(f"Successfully saved {len(records)} records to {filename}")


get_latest()
