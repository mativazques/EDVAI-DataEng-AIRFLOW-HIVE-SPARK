from pyspark.sql.functions import col, avg
from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .appName("Sqoop to Hive") \
    .config("spark.sql.catalog.Implementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.option("header","true").parquet("hdfs://172.17.0.2:9000/sqoop/ingest/northwind/clientes/*.parquet")

#df.show(10)
#df.describe().show()
#df.printSchema()

avg_quantity = df.select(avg("quantity")).collect()[0][0]

df_filtered = df.filter(col("quantity") > avg_quantity)

df_filtered.show(10)

df_filtered.write.mode("overwrite").saveAsTable("northwind_analytics.products_sold")

#ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/clase9/transform/processing_table_1.py