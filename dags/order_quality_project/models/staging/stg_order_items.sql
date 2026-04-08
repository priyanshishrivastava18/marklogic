{{ config(materialized='table') }}
SELECT * FROM stg_order_items