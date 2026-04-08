import requests
import pandas as pd
from sqlalchemy import create_engine
from requests.auth import HTTPDigestAuth

# -------------------------------
# PostgreSQL Connection
# -------------------------------
engine = create_engine(
    "postgresql://postgres:newpassword123@localhost:5432/order_quality_analytics_db"
)

# -------------------------------
# Fetch Single JSON File from MarkLogic
# -------------------------------
def fetch_data(uri):
    url = f"http://localhost:8000/v1/documents?uri={uri}&format=json"
    
    response = requests.get(
        url,
        auth=HTTPDigestAuth("admin", "Priyanshi@32835")
    )
    
    print(f"\nFetching: {uri}")
    print("STATUS:", response.status_code)

    if response.status_code != 200:
        print(response.text)
        raise Exception(f"Failed to fetch data for {uri}")
    
    data = response.json()
    return pd.json_normalize(data)

# -------------------------------
# Load Function
# -------------------------------
def load_table(table_name, uri):
    try:
        df = fetch_data(uri)

        print(f"Loading {table_name}... Rows: {len(df)}")

        df.to_sql(table_name, engine, if_exists="replace", index=False)

        print(f"Loaded {table_name}")

    except Exception as e:
        print(f"Error loading {table_name}: {e}")


# -------------------------------
# MAIN EXECUTION
# -------------------------------
if __name__ == "__main__":

    load_table("stg_customers", "/silver/customers/all_customers.json")
    load_table("stg_orders", "/silver/orders/all_orders.json")
    load_table("stg_payments", "/silver/payments/all_payments.json")
    load_table("stg_shipments", "/silver/shipments/all_shipments.json")
    load_table("stg_order_items", "/silver/order_items/all_order_items.json")

    print("\nALL DATA LOADED SUCCESSFULLY!")