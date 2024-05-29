from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .appName("Sqoop to Hive") \
    .config("spark.sql.catalog.Implementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

df3 = spark.read.option("header","true").parquet("hdfs://172.17.0.2:9000/sqoop/ingest/northwind/envios/*.parquet")
df3.createOrReplaceTempView("vw_df3")
df4 = spark.read.option("header","true").parquet("hdfs://172.17.0.2:9000/sqoop/ingest/northwind/order_details/*.parquet")
df4.createOrReplaceTempView("vw_df4")

df_filtered = spark.sql(" \
SELECT a.order_id, a.shipped_date, a.company_name, a.phone, \
    (b.unit_price - b.discount) as unit_price_discount, b.quantity, ((b.unit_price - b.discount) * b.quantity) as total_price \
FROM vw_df3 as a \
JOIN vw_df4 as b ON b.order_id = a.order_id \
WHERE b.discount > 0 \
")

df_filtered.show(2)
#df.show(10)
#df.describe().show()
#df.printSchema()

#df_filtered.show(10)

df_filtered.write.mode("overwrite").saveAsTable("northwind_analytics.products_sent")

#ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/clase9/transform/processing_table_2_3.py