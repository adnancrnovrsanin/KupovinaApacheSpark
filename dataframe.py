"""
This module reads a CSV file into a Spark DataFrame, renames columns, 
casts a column to double, calculates the total spent by each customer, 
and displays the result.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

spark = SparkSession.builder.appName("DistribuiraniSistemi").master("local[*]").getOrCreate()

PATH = "kupovina.csv"

df = spark.read.option("header", "false").csv(PATH)

df = df.withColumnRenamed("_c0", "customerId") \
       .withColumnRenamed("_c1", "productId") \
       .withColumnRenamed("_c2", "price")

df = df.withColumn("price", col("price").cast("double"))

total_spent_by_customer = df.groupBy("customerId") \
                            .agg(sum("price").alias("totalSpent"))

total_spent_by_customer = total_spent_by_customer.withColumn("totalSpent",
                                                               round(col("totalSpent"), 2))

total_spent_by_customer.show(1000)
