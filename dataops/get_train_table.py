import pandas as pd
from sqlalchemy import create_engine

# 1. Setup your connection (Adjust the URI for your specific DB: PostgreSQL, Snowflake, etc.)
USER, PASSWORD, HOST, DB = "bt4301", "password", "localhost", "customer_churn"
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}")

# 2. Define the transformation query
query = """
SELECT
    f.customer_id,
    f.churn,
    c.annual_income,
    a.contract,
    s.has_phone_service,
    s.has_internet_service,
    s.has_tech_support,
    s.has_streaming_tv,
    s.has_streaming_movies,
    b.customer_satisfaction,
    b.num_complaints,
    b.num_service_calls,
    b.late_payments,
    b.high_risk_flag,
    c.age_group,
    c.tenure_segment,
    a.is_auto_pay

FROM fact_customer_churn f
LEFT JOIN dim_customer  c ON f.customer_id = c.customer_id
LEFT JOIN dim_account   a ON f.customer_id = a.customer_id
LEFT JOIN dim_service   s ON f.customer_id = s.customer_id
LEFT JOIN dim_behavior  b ON f.customer_id = b.customer_id
"""

df = pd.read_sql(query, engine)

print(f"Writing {len(df)} rows to train_churn_model...")
df.to_sql(
    "train_churn_model", engine, if_exists="replace", index=False, chunksize=10000
)

print("Success: Table 'train_churn_model' created.")
