import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# Database Connection Configuration
USER, PASSWORD, HOST, DB = "bt4301", "password", "localhost", "customer_churn"
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}")


def engineer_customer_features():
    # Load raw data
    query = """
    SELECT 
        c.customer_id, c.signup_date, c.age, c.gender, c.annual_income, 
        c.education, c.marital_status, c.dependents, c.senior_citizen,
        a.tenure
    FROM dim_customer c
    INNER JOIN dim_account a ON c.customer_id = a.customer_id
    """

    df = pd.read_sql(query, engine)

    # Create Tenure Segments -- (0-6 months, 6-24, 24-60, 60-100)
    bins = [0, 6, 24, 60, 100]
    labels = ["Infant", "Stable", "Loyal", "Veteran"]
    df["tenure_segment"] = pd.cut(df["tenure"], bins=bins, labels=labels)

    print(df["tenure_segment"].unique())

    # Create Age Segments
    age_bins = [0, 18, 30, 40, 50, 60, 70, 100]
    age_labels = ["0-18", "18-29", "30-39", "40-49", "50-59", "60-69", "70+"]
    df["age_group"] = pd.cut(df["age"], bins=age_bins, labels=age_labels, right=True)

    print(df["age_group"].unique())

    # # Create household size feature -- Combine marital status and dependents into a single score
    # df['is_married'] = (df['marital_status'] == 'Married').astype(int)
    # df['household_size'] = df['is_married'] + df['dependents'] + 1

    # Write back to MySQL (replacing the table or adding columns)
    df.to_sql("dim_customer", engine, if_exists="replace", index=False, chunksize=10000)

    print("Successfully updated dim_customer in local MySQL.")


# Execute
engineer_customer_features()
