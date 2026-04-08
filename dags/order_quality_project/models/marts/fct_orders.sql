SELECT 
    o.order_id,
    o.customer_id,
    
    p.amount AS payment_amount,
    s.delivery_status

FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_payments') }} p
ON o.order_id = p.order_id
LEFT JOIN {{ ref('stg_shipments') }} s
ON o.order_id = s.order_id