
select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,

    -- amount stored as cents, convert it to dollars
    amount / 100 as amount,
    created as created_at

from {{ source('jaffle_shop', 'payment') }}