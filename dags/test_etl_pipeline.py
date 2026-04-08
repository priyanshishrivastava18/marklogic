# # test_etl_pipeline_all_tables.py
# import pytest
# import pandas as pd
# import great_expectations as ge
# # import sqlalchemy  # Optional if you want Postgres test

# # ------------------------------
# # Mock ETL Transform Functions
# # ------------------------------
# def apply_mark_logic_customers(df):
#     df = df.copy()
#     df["record_status"] = "valid"
#     for i, row in df.iterrows():
#         if pd.isnull(row.get("customer_id")) or pd.isnull(row.get("email")):
#             df.at[i, "record_status"] = "invalid"
#     return df

# def apply_mark_logic_orders(df):
#     df = df.copy()
#     df["record_status"] = "valid"
#     for i, row in df.iterrows():
#         if pd.isnull(row.get("order_id")) or pd.isnull(row.get("customer_id")):
#             df.at[i, "record_status"] = "invalid"
#         elif row.get("order_amount", 0) > 1000:
#             df.at[i, "record_status"] = "high_value"
#         elif row.get("payment_status") == "failed":
#             df.at[i, "record_status"] = "payment_failed"
#         elif pd.notnull(row.get("delivered_date")) and pd.notnull(row.get("expected_delivery_date")):
#             if row["delivered_date"] > row["expected_delivery_date"]:
#                 df.at[i, "record_status"] = "late_delivery"
#     return df

# def apply_mark_logic_payments(df):
#     df = df.copy()
#     df["record_status"] = "valid"
#     for i, row in df.iterrows():
#         if row.get("payment_status") == "failed":
#             df.at[i, "record_status"] = "payment_failed"
#     return df

# def apply_mark_logic_shipments(df):
#     df = df.copy()
#     df["record_status"] = "valid"
#     for i, row in df.iterrows():
#         if pd.notnull(row.get("delivered_date")) and pd.notnull(row.get("expected_delivery_date")):
#             if row["delivered_date"] > row["expected_delivery_date"]:
#                 df.at[i, "record_status"] = "late_delivery"
#     return df

# # ------------------------------
# # Unit Tests for Each Table
# # ------------------------------
# def test_customers_mark_logic():
#     data = pd.DataFrame([
#         {"customer_id": 101, "email": "c1@test.com"},
#         {"customer_id": None, "email": "c2@test.com"}
#     ])
#     result = apply_mark_logic_customers(data)
#     assert result.loc[0, "record_status"] == "valid"
#     assert result.loc[1, "record_status"] == "invalid"

# def test_orders_mark_logic():
#     data = pd.DataFrame([
#         {"order_id": 1, "customer_id": 101, "order_amount": 500, "payment_status": "success"},
#         {"order_id": 2, "customer_id": 102, "order_amount": 2000, "payment_status": "success"},
#         {"order_id": 3, "customer_id": 103, "order_amount": 300, "payment_status": "failed"},
#         {"order_id": 4, "customer_id": 104, "order_amount": 400,
#          "payment_status": "success", "expected_delivery_date": "2026-04-10", "delivered_date": "2026-04-12"}
#     ])
#     result = apply_mark_logic_orders(data)
#     assert set(result["record_status"]) == {"valid", "high_value", "payment_failed", "late_delivery"}

# def test_payments_mark_logic():
#     data = pd.DataFrame([
#         {"payment_id": 1, "payment_status": "success"},
#         {"payment_id": 2, "payment_status": "failed"}
#     ])
#     result = apply_mark_logic_payments(data)
#     assert result.loc[0, "record_status"] == "valid"
#     assert result.loc[1, "record_status"] == "payment_failed"

# def test_shipments_mark_logic():
#     data = pd.DataFrame([
#         {"shipment_id": 1, "expected_delivery_date": "2026-04-10", "delivered_date": "2026-04-09"},
#         {"shipment_id": 2, "expected_delivery_date": "2026-04-10", "delivered_date": "2026-04-12"}
#     ])
#     result = apply_mark_logic_shipments(data)
#     assert result.loc[0, "record_status"] == "valid"
#     assert result.loc[1, "record_status"] == "late_delivery"

# # ------------------------------
# # Great Expectations Data Quality
# # ------------------------------
# def test_data_quality_orders():
#     df = pd.DataFrame([
#         {"order_id": 1, "customer_id": 101, "order_amount": 500},
#         {"order_id": 2, "customer_id": 102, "order_amount": 2000}
#     ])
#     dataset = ge.from_pandas(df)
#     assert dataset.expect_column_values_to_not_be_null("order_id").success
#     assert dataset.expect_column_values_to_not_be_null("customer_id").success
#     assert dataset.expect_column_values_to_be_between("order_amount", min_value=0).success




# import pytest
# import pandas as pd

# def apply_mark_logic_orders(df):
#     df = df.copy()
#     df["record_status"] = "valid"
#     for i, row in df.iterrows():
#         if pd.isnull(row.get("order_id")) or pd.isnull(row.get("customer_id")) or pd.isnull(row.get("order_date")):
#             df.at[i, "record_status"] = "invalid"
#     return df

# def test_orders_mark_logic():
#     data = pd.DataFrame([
#         {"order_id": 11075, "customer_id": 2994, "order_date": "2024-08-14"},
#         {"order_id": None, "customer_id": 3000, "order_date": "2024-08-15"},
#     ])
#     result = apply_mark_logic_orders(data)
#     assert result.loc[0, "record_status"] == "valid"
#     assert result.loc[1, "record_status"] == "invalid"

# test_etl_pipeline.py
# test_etl_pipeline.py
# test_etl_pipeline.py
import pytest
import pandas as pd
from sqlalchemy import create_engine

def test_fct_orders_loaded():
    """
    Verify that fct_orders table is populated after ETL from MarkLogic.
    """
    # SQLAlchemy engine
    engine = create_engine(
        "postgresql+psycopg2://postgres:newpassword123@localhost/order_quality_analytics_db"
    )

    # Fetch data from fct_orders
    df_pg = pd.read_sql(
        "SELECT order_id, customer_id, payment_amount, delivery_status FROM fct_orders ORDER BY order_id",
        engine
    )

    # Basic checks
    assert not df_pg.empty, "fct_orders table is empty! ETL might have failed."

    # Example checks: valid order_id present
    assert 11075 in df_pg['order_id'].values, "Expected order_id 11075 not found."
    
    # If you know which order should be invalid, check delivery_status or payment_amount
    invalid_rows = df_pg[df_pg['payment_amount'].isnull()]
    assert not invalid_rows.empty, "Expected some invalid rows with null payment_amount."

    print("✅ fct_orders test: table loaded correctly with MarkLogic ETL data")