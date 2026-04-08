SELECT 
    o.order_id,
    o.customer_id,
    o.order_status,
    p.payment_flag,

    CASE 
        WHEN p.payment_flag = 'failed' THEN 'payment_failed'
        WHEN o.order_status = 'duplicate' THEN 'duplicate'
        WHEN o.order_status = 'invalid' THEN 'invalid'
        ELSE 'valid'
    END AS final_status

FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_payments') }} p
ON o.order_id = p.order_id