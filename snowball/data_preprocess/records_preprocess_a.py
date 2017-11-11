# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
import pyspark.sql.functions as psf

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    records = spark.read.csv('data/records.csv', header=True, inferSchema=True)
    #records.printSchema()
    quote = spark.read.csv('data/quote.csv', header=True, inferSchema=True)
    #quote.printSchema()

    #records.groupBy('PortCode','SecuCode', to_date(psf.col('Updated')).alias('Date')).count().orderBy('PortCode','Date','SecuCode').show()
    #records.join(quote, (quote.SecuCode == records.SecuCode)&(to_date(records.Updated)==to_date(quote.TradingDay)))\
    #    .select('PortCode', 'Updated', to_date('Updated').alias('Date'), records.SecuCode, 'PrevWeight', 'TargetWeight', 'ChangePCT')\
    #    .orderBy('PortCode','Updated','Date','SecuCode')\
    #    .groupBy('PortCode','Date','SecuCode')\
    #    .agg(last('Updated')) \
    #    .orderBy('PortCode', 'Date', 'Date', 'SecuCode') \
    #    .select('*').show()
    mergedRecords = records.select('PortCode', 'Updated', to_date('Updated').alias('Date'), 'SecuCode','PrevWeight', 'TargetWeight')\
        .groupBy('PortCode','SecuCode','Date')\
        .agg(first('PrevWeight').alias('InitWeight'),last('TargetWeight').alias('EndWeight'))\
        .orderBy('PortCode','Date','SecuCode')\
        .select('*', (psf.col('EndWeight')-psf.col('InitWeight')).alias('WeightChange'))

    padZero = udf(lambda e:str(e).zfill(6))
    joinedRecords = mergedRecords.join(quote,(quote.SecuCode == mergedRecords.SecuCode)&(mergedRecords.Date == to_date(quote.TradingDay)))\
    .select('PortCode',padZero(quote.SecuCode).alias('SecuCode'),'Date','InitWeight','EndWeight','WeightChange','SecuAbbr',psf.col('ChangePCT').alias('SecuChangePCT')) \
    .orderBy('PortCode', 'Date', 'SecuCode') \

    joinedRecords.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save('dist/records_preprocess_a.csv')




