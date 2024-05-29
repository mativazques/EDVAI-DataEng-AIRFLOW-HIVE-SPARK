/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password-file file:///home/hadoop/sqoop/scripts/sqoop.pass \
--query "SELECT o.order_id, o.shipped_date, c.company_name, c.postal_code 
FROM orders o 
JOIN customers c on c.customer_id = o.customer_id 
WHERE \$CONDITIONS " \
--m 1 \
--target-dir /sqoop/ingest/northwind/envios \
--as-parquetfile \
--delete-target-dir