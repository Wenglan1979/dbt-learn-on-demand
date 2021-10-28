with orders as  (
    select * from {{ ref('stg_orders' )}}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount

    from payments
    group by 1
),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(order_payments.amount, 0) as amount

    from orders
    left join order_payments using (order_id)
)

select * from final


/* with fct_orders as (

    select
        o.id as order_id,
        o.user_id as customer_id,
        p.amount

    from `driven-crane-330213.jaffle_shop.orders` o
    left join `driven-crane-330213.jaffle_shop.payment` p on o.id=p.order_id
)

select * from fct_orders

*/