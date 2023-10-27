with prod as(
    select 
        ct.category_name,
        sp.company_name as suppliers,
        pd.product_name,
        pd.unit_price,
        pd.product_id
    from {{source('sources', 'products')}} as pd
    left join {{source('sources', 'suppliers')}} as sp on (pd.supplier_id = sp.supplier_id)
    left join {{source('sources', 'categories')}} as ct on (pd.category_id = ct.category_id)
), orderDetails as (
    select 
        pd.*, 
        od.order_id, 
        od.quantity, 
        od.discount
    from {{ref('order_details')}} as od
    left join prod as pd on (od.product_id = pd.product_id) 
), orders as (
    select
        ord.order_date,
        ord.order_id,
        cs.company_name as customer,
        emp.name employee,
        emp.age,
        emp.lengthOfService
    from {{source('sources','orders')}} as ord
    left join {{ref('customers')}} as cs on (ord.customer_id = cs.customer_id)
    left join {{ref('employees')}} as emp on (ord.employee_id = emp.employee_id)
    left join {{source('sources', 'shippers')}} sh on (ord.ship_via = sh.shipper_id)
)
select 
    orders.order_date,
    orders.customer,
    orders.employee,
    orders.age,
    orders.lengthOfService,
    orderDetails.*
from orders
inner join orderDetails on orders.order_id = orderDetails.order_id
order by orderDetails.order_id
