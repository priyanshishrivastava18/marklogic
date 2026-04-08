# =========================================
# IMPORTS
# =========================================
import boto3
import pandas as pd
from io import StringIO
import json
import requests


# =========================================
# CONFIG
# =========================================
BUCKET_NAME = "marklogic-bucket1"

AWS_CONFIG = {
    "aws_access_key_id": "AKIAQJ2PSTCN5YTKY6DL",
    "aws_secret_access_key": "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",
    "region_name": "ap-south-1"
}


MARKLOGIC_CONFIG = {
    "url": "http://localhost:8000/v1/documents",
    "username": "admin",
    "password": "Priyanshi@32835"   # 👈 your password
}


# =========================================
# EXTRACT: READ CSV FROM S3
# =========================================
def read_s3_csv(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client('s3', **AWS_CONFIG)

    response = s3.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')

    return pd.read_csv(StringIO(data))


# =========================================
# TRANSFORM: CONVERT DATAFRAMES TO JSON
# =========================================
def convert_multiple_to_json(dfs: dict) -> dict:
    json_result = {}

    for name, df in dfs.items():
        df.columns = df.columns.str.lower()
        df = df.where(pd.notnull(df), None)

        json_result[name] = df.to_dict(orient="records")

    return json_result


# =========================================
# LOAD: INSERT INTO MARKLOGIC (CORRECT)
# =========================================
from requests.auth import HTTPDigestAuth
import json

def load_to_marklogic(json_data):
    url = "http://localhost:8000/v1/documents"
    auth = HTTPDigestAuth("admin", "Priyanshi@32835")

    for collection, records in json_data.items():
        print(f"Loading {collection}...")

        uri = f"/{collection}.json"   # 👈 ONE FILE

        response = requests.put(
            url,
            params={
                "uri": uri,
                "collection": collection
            },
            auth=auth,
            headers={"Content-Type": "application/json"},
            data=json.dumps(records)   # full list as ONE doc
        )

        if response.status_code in [200, 201]:
            print(f"{collection} loaded successfully\n")
        else:
            print(f"Error {response.status_code} for {collection}")

# =========================================
# MAIN PIPELINE
# =========================================
def main():
    print("Reading data from S3...")

    customers_df = read_s3_csv(BUCKET_NAME, "customers.csv")
    orders_df = read_s3_csv(BUCKET_NAME, "orders.csv")
    order_items_df = read_s3_csv(BUCKET_NAME, "order_items.csv")
    payments_df = read_s3_csv(BUCKET_NAME, "payment.csv")
    shipments_df = read_s3_csv(BUCKET_NAME, "shipments.csv")

    print("Data loaded\n")

    dfs = {
        "customers": customers_df,
        "orders": orders_df,
        "order_items": order_items_df,
        "payments": payments_df,
        "shipments": shipments_df
    }

    print("Converting to JSON...")
    json_data = convert_multiple_to_json(dfs)
    print("Conversion done\n")

    print("Loading into MarkLogic...")
    load_to_marklogic(json_data)
    print("Data loaded successfully")


# =========================================
# ENTRY POINT
# =========================================
if __name__ == "__main__":
    main()