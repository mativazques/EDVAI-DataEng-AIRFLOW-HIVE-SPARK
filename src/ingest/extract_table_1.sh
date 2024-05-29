/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password-file file:///home/hadoop/sqoop/scripts/sqoop.pass \
--query "SELECT c.customer_id, c.company_name, od.quantity productos_vendidos
FROM order_details od 
JOIN orders o ON o.order_id = od.order_id 
JOIN customers c ON c.customer_id = o.customer_id 
WHERE \$CONDITIONS 
ORDER BY od.quantity DESC" \
--m 1 \
--target-dir /sqoop/ingest/northwind/clientes \
--as-parquetfile \
--delete-target-dir