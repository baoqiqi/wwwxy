# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
import pyspark.sql.functions as psf

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    active_trader = spark.read.csv('dist/records_preprocess_a.output.csv', header=True, inferSchema=True).groupBy('PortCode').count().select('*').orderBy('count').where('count>10')
    active_trader.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save('dist/records_preprocess_b.csv')




