with lowest_price_by_vendor as (
    select vendor, min(sale_price) as price, count(items) as num_items
    from items 
    where stock_status = 'In Stock' and category = 'Electronics'
    group by 1
)
select 
    lpbv.vendor, lpbv.num_items,
    i.item || ' - $' || lpbv.price as cheapest_item
from lowest_price_by_vendor lpbv
inner join highest_price_by_vendor hpbv
inner join items i
on lpbv.vendor = i.vendor and lpbv.price = i.sale_price
