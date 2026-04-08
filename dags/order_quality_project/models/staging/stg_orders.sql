{{ config(materialized='table') }}
SELECT * FROM stg_orders