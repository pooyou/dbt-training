select
-- from raw_ orders
{{ dbt_utils.generate_surrogate_key(['o.orderid', 'c.customerid', 'p.productid']) }} as sk_orders,
o.orderid,
o.orderdate,
o.shipdate,
o.shipmode,
o.ordersellingprice - o.ordercost as orderprofit,
o.ordercost,
o.ordersellingprice,
-- From raw customer
c.customerid,
c.customername,
c.segment,
c.country,
-- From product
p.productid,
p.category,
p.productname,
p.subcategory,
{{ markup('o.ordersellingprice', 'o.ordercost') }} as markup,
d.delivery_team

from {{ ref("raw_orders") }} as o
left join {{ ref('raw_customer') }} as c
ON o.customerid = c.customerid
left join {{ ref('raw_product') }} as p
ON o.productid = p.productid
LEFT JOIN {{ ref('delivery_team') }} d
ON o.shipmode = d.shipmode

{{limit_data_in_dev('orderdate')}}