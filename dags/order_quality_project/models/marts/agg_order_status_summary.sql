SELECT 
    final_status,
    COUNT(*) AS total_orders

FROM {{ ref('int_order_validation') }}

GROUP BY final_status