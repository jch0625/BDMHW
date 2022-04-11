from pyspark import SparkContext
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":
	sc = SparkContext()


def main(sc):
	
   	KFP = '/tmp/bdm/keyfood_products.csv'
    	dffkp = spark.read.load(KFP, format='csv',header = True, inferSchema = True)
    	dfkns = pd.read_json('keyfood_nyc_stores.json')
    	dfkns = dfkns.T
    	dfkns1 = dfkns[['name','communityDistrict','foodInsecurity']]
    	dfkns2 = spark.createDataFrame(dfkns1)

    	dfksi = pd.read_csv('keyfood_sample_items.csv')
    	dfksi['UPC code'] = dfksi['UPC code'].apply(lambda x : x.split('-')[1])
   	dfksi.columns = ['upc','ItemName']

    	outputTask1 = ddffkp
	sc.textFile(outputTask1)
if __name__ == "__main__":
	sc = SparkContext()
	main(sc)
