CREATE MATERIALIZED VIEW IF NOT EXISTS agg_items_by_category_vendor_stock_status AS 
SELECT 
	category, vendor, stock_status,
	avg(sale_price) as avg_sale_price, 
	avg(sale_price + shipping_cost) as avg_total_price, 
	avg(shipping_cost) as avg_cost, 
	max(sale_price) as max_sale_price, 
	max(sale_price + shipping_cost) as max_total_price, 
	max(shipping_cost) as max_shipping_cost, 
	min(sale_price) as min_sale_price, 
	min(sale_price + shipping_cost) as min_shipping_cost
FROM items 
GROUP BY 1,2,3;