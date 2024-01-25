import unittest
from pyspark import SparkConf, SparkContext

import shutil
import os

temp_dir = "C:\\Users\\adnan\\AppData\\Local\\Temp\\"
for dir_name in os.listdir(temp_dir):
    if dir_name.startswith("spark-"):
        shutil.rmtree(os.path.join(temp_dir, dir_name), ignore_errors=True)

class RDDTest(unittest.TestCase):
    def setUp(self):
        conf = SparkConf().setAppName("DistribuiraniSistemi").setMaster("local[*]")
        self.sc = SparkContext(conf=conf)
        self.path = "kupovina.csv"
        
    def tearDown(self):
        self.sc.stop()
        
    def test_total_spent_by_customer(self):
        input_rdd = self.sc.parallelize([
            "1,Product A,10.0",
            "2,Product B,20.0",
            "1,Product C,15.0",
            "3,Product D,5.0",
            "2,Product E,25.0"
        ])
        
        purchases = input_rdd.map(lambda line: line.split(",")) \
            .map(lambda parts: (parts[0], float(parts[2])))

        total_spent_by_customer = purchases.reduceByKey(lambda x, y: x + y)
        total_spent_by_customer = total_spent_by_customer.mapValues(lambda x: round(x, 2))

        expected_output = [
            ("2", 45.0),
            ("3", 5.0),
            ("1", 25.0)
        ]
        
        self.assertEqual(total_spent_by_customer.collect(), expected_output)

if __name__ == '__main__':
    unittest.main()