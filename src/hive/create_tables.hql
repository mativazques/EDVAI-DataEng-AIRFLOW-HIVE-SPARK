USE northwind_analytics;
CREATE TABLE products_sold(
    customer_id int, 
    company_name string,
    productos_vendidos  int
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';
CREATE TABLE products_sent(
    order_id int,
    shhipped_date date ,
    company_name string, 
    phone int,
    unit_price_discount float, 
    quantity int, 
    total_price float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';