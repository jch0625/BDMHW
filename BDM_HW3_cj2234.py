from pyspark import SparkContext
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


def main(sc):
    KFP = '/tmp/bdm/keyfood_products.csv'
    dffkp = spark.read.load(KFP, format='csv',header = True, inferSchema = True)

    df1 = dffkp.withColumn('newupc',split(dffkp['upc'],'-').getItem(1))
    df2 = df1.withColumn("price1", regexp_extract('price', "([+-]?([0-9]*[.])?[0-9]+)" , 1 ))

    df3= df2.select('store', 'department',df2['newupc'].alias('upc'),'product','size',df2['price1'].alias('price').cast('float'))

    dfkns = pd.read_json('keyfood_nyc_stores.json')
    dfkns = dfkns.T
    dfkns1 = dfkns[['name','communityDistrict','foodInsecurity']]

    dfkns2 = spark.createDataFrame(dfkns1)

    dfksi = pd.read_csv('keyfood_sample_items.csv')
    dfksi['UPC code'] = dfksi['UPC code'].apply(lambda x : x.split('-')[1])
    dfksi.columns = ['upc','ItemName']

    df4 = df3.join(dfksi1, df3.upc == dfksi1.upc, how = 'inner')

    df5 = df4.join(dfkns2, df4.store == dfkns2.name, how = 'inner')

    outputTask1 = df5.withColumn('food_insecurity',(df5[10]*100).cast('int')).select('ItemName','price','food_insecurity')
if __name__ == "__main__":
 sc = SparkContext()
 main(sc)
