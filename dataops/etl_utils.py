import mysql.connector
import pandas as pd
from sqlalchemy import create_engine


TARGET_DB_URL = 'mysql://bt4301:password@localhost:3306/datawarehouse'

def extract_data():
    conn = mysql.connector.connect(
        host='localhost',
        user='bt4301',
        passwd='password',
        database='adventureworks2012'
    )
    
    query = "SELECT CustomerID, TerritoryID, AccountNumber FROM customer LIMIT 10;"
    df = pd.read_sql(query, con=conn)
    
    return df


def load_data(df):

    target_engine = create_engine(TARGET_DB_URL, echo=False)

    df.to_sql(
        name="customer_sample",
        con=target_engine,
        if_exists="replace",
        index=False
    )

    print("Data loaded successfully into datawarehouse.customer_sample")