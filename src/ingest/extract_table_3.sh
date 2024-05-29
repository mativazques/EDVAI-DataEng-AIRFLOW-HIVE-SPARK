/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password-file file:///home/hadoop/sqoop/scripts/sqoop.pass \
--query "select o.order_id, od.unit_price, od.quantity, od.discount 
from orders o 
join order_details od on od.order_id = o.order_id 
WHERE \$CONDITIONS " \
--m 1 \
--target-dir /sqoop/ingest/northwind/order_details \
--as-parquetfile \
--delete-target-dir