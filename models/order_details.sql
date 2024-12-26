with cte_1 as (
SELECT
    od.order_id, od.product_id, od.unit_price, od.quantity, p.product_name, p.supplier_id, p.category_id,
    od.unit_price * od.quantity AS total,
    (p.unit_price * od.quantity) - total AS total_discount
FROM
    {{source("sources", "order_details")}} od 
    LEFT JOIN {{source("sources","products")}} p on (od.product_id = p.product_id)
)
SELECT
    *
FROM
    cte_1
