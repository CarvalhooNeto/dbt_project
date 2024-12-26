WITH prod as (
SELECT  
    ct.category_id, sp.company_name supplier, pd.product_name, pd.unit_price, pd.product_id
FROM
    {{source('sources', 'products')}} pd LEFT JOIN {{source('sources', 'suppliers')}} sp ON (pd.supplier_id = sp.supplier_id)
                                         LEFT JOIN {{source('sources', 'categories')}} ct ON (pd.category_id = ct.category_id)
), orddetai as (
    SELECT
        p.*, od.order_id, od.quantity, od.total_discount
    from 
        {{ref('order_details')}} od LEFT JOIN prod p ON (od.product_id = p.product_id)
), ords as(
    SELECT
        ord.order_date, ord.order_id, c.company_name customer, e.fullname employee, e.age, e.lenght_of_service
    FROM   
        {{source('sources', 'orders')}} ord LEFT JOIN {{ref("customers")}} c ON (ord.customer_id = c.customer_id)
                                            LEFT JOIN {{ref("employees")}} e ON (ord.employee_id = e.employee_id)
                                            LEFT JOIN {{source("sources","shippers")}} s ON (ord.ship_via = s.shipper_id)
), final_Join as(
    SELECT
        ordt.*,ord.order_date, ord.customer, ord.employee, ord.age, ord.lenght_of_service
    FROM    
        orddetai ordt INNER JOIN ords ord ON (ordt.order_id = ord.order_id) 
)
SELECT
    *
from 
    final_join