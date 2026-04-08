SELECT 
    payment_id,
    order_id,
    amount,
    payment_flag,

    CASE 
        WHEN payment_flag = 'failed' THEN 'payment_failed'
        ELSE 'valid'
    END AS payment_status_final

FROM {{ ref('stg_payments') }}