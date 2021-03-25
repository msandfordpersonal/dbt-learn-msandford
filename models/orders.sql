{{
  config(
    materialized='view'
  )
}}

-- with orders as --cte for orders
-- (
--     select 
--          order_id
--         ,customer_id
--     from {{ ref('stg_orders')}} 
-- )

with orders as --cte for orders
(
    select 
         order_id
        ,customer_id
        ,order_date
        ,status
    from {{ ref('stg_orders')}} 
)

,payments as --cte for payments
(
    select 
         order_id
        ,sum(payment_amount) as amount
    from {{ ref('stg_payments')}} 
    where payment_status = 'success'
    group by 1
)
-- ,final as
-- (
select 
     pay.order_id
    ,ord.customer_id
    ,amount
    ,order_date
    ,status
from payments pay 
left join orders ord
on pay.order_id = ord.order_id
-- )
-- select sum(amount) from final