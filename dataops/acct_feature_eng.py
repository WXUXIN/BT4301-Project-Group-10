import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# Database Connection Configuration
USER, PASSWORD, HOST, DB = 'bt4301', 'password', 'localhost', 'customer_churn'
engine = create_engine(f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}')

def engineer_customer_features():
    # Load raw data
    query = """
    SELECT * FROM dim_account 
    """

    df = pd.read_sql(query, engine)
    # print(df.columns)
    
    # Create Autopay feature
    autopay_methods = {'credit_card', 'bank_transfer'}
    df['is_auto_pay'] = df['payment_method'].isin(autopay_methods).astype(int)

    # Write back to MySQL (replacing the table or adding columns)
    df.to_sql('dim_account', engine, if_exists='replace', index=False)

    print("Successfully updated dim_account in local MySQL.")

# Execute
engineer_customer_features()