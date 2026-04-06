import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# Database Connection Configuration
USER, PASSWORD, HOST, DB = 'bt4301', 'password', 'localhost', 'customer_churn'
engine = create_engine(f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}')

def engineer_customer_features(dry_run=True):
    # Load raw data
    df = pd.read_sql("SELECT * FROM dim_customer", engine)
    
    # Create Tenure Segments -- (0-6 months, 6-24, 24-60, 60-100)
    # df['signup_date'] = pd.to_datetime(df['signup_date'])
    df['tenure_months'] = ((pd.Timestamp.now() - df['signup_date']).dt.days / 30.44).astype(int)    
    bins = [0, 6, 24, 60, 100]
    labels = ['Infant', 'Stable', 'Loyal', 'Veteran']
    df['tenure_segment'] = pd.cut(df['tenure_months'], bins=bins, labels=labels)

    # Create household size feature -- Combine marital status and dependents into a single score
    df['is_married'] = (df['marital_status'] == 'Married').astype(int)
    df['household_size'] = df['is_married'] + df['dependents'] + 1


    if dry_run:
        print("Dry Run Results (First 5 rows):")
        print(df[['customer_id', 'tenure_segment', 'household_size']].head())
    else:
        # Write back to MySQL (replacing the table or adding columns)
        df.to_sql('dim_customer', engine, if_exists='replace', index=False)
        print("Successfully updated dim_customer in local MySQL.")

# Execute
engineer_customer_features(dry_run=False)