# This is a sample Python script.
import os
import sys
HOME_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(HOME_DIR)
print(sys.path)
from utils import dataUtils
from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()

if __name__ == "__main__":
    du = dataUtils.DataUtils()
    df01 = du.read_db("emp_test01")
    df01.createOrReplaceTempView("tmp_table")
    spark.sql("SELECT * FROM tmp_table").show()
