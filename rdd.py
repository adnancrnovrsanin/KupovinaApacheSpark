"""
This module performs operations on a Spark RDD loaded from a CSV file.
"""

import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("DistribuiraniSistemi").setMaster(
    "local[*]")
sc = SparkContext(conf=conf)

PATH = "kupovina.csv"
input_rdd = sc.textFile(PATH)

purchases = input_rdd.map(lambda line: line.split(",")) \
    .map(lambda parts: (parts[0], float(parts[2])))

total_spent_by_customer = purchases.reduceByKey(lambda x, y: x + y)
total_spent_by_customer = total_spent_by_customer.mapValues(lambda x: round(x, 2))

print("+++++++++++++++++++++++++++")
print("+Customer ID | Total Spent+")
for customer, total_spent in total_spent_by_customer.collect():
    print(f"+ {customer} | {total_spent} +")

sys.exit(0)
