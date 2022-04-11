from pyspark import SparkContext
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


def read_file(sc):
	KFP = 'keyfood_products.csv'
    	dffkp = spark.read.load(KFP, format='csv',header = True, inferSchema = True)

   	df1 = dffkp.withColumn('newupc',split(dffkp['upc'],'-').getItem(1))
    	df2 = df1.withColumn("price1", regexp_extract('price', "([+-]?([0-9]*[.])?[0-9]+)" , 1 ))

    	
    	outputTask1 = df2
    	rdd=outputTask1.rdd
    	return rdd



def main(sc):
	df = read_file(sc)
	sc.textFile(df.saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'output')
	

if __name__ == "__main__":
	sc = SparkContext()
	main(sc)
