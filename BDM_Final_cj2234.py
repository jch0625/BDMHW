import pandas as pd
from google.colab import drive
import math
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import DateType, IntegerType, MapType, StringType,FloatType,StructType,StructField,ArrayType
from pyspark.sql import Row, Window
from pyspark.sql.functions import col
import pyproj


sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)
spark

df = pd.read_csv('nyc_cbg_centroids.csv')
df1 = pd.read_csv('nyc_supermarkets.csv')
df2 = spark.read.load('/tmp/bdm/weekly-patterns-nyc-2019-2020', format='csv',header = True, inferSchema = True)

dfcbg = df
dfcbg['cbg_fips'] = dfcbg['cbg_fips'].astype('string')
dfcbg1 = dfcbg[dfcbg['cbg_fips'].str.startswith('36061')|dfcbg['cbg_fips'].str.startswith('36005')|dfcbg['cbg_fips'].str.startswith('36047')\
      |dfcbg['cbg_fips'].str.startswith('36081')|dfcbg['cbg_fips'].str.startswith('36085')]
dfcbg1['cbg_fips'] = dfcbg1['cbg_fips'].astype('int')

dfpat = df2[['placekey','poi_cbg','visitor_home_cbgs','date_range_end','date_range_start']]
dfpat1 = spark.createDataFrame(dfpat)
dfsup = spark.createDataFrame(df1)
dfsup = dfsup.withColumn("safegraph_placekey",dfsup.safegraph_placekey.cast('string'))
dfpat11 = dfpat1.join(dfsup, dfpat1.placekey == dfsup.safegraph_placekey, how = 'inner')
dfnew = dfpat11.join(dfcbg2, dfpat11.poi_cbg == dfcbg2.cbg_fips, how = 'inner')
dfnew = dfnew.select('placekey','poi_cbg','visitor_home_cbgs','date_range_end','date_range_start')
dfnew = dfnew.withColumn("poi_cbg",dfnew.poi_cbg.cast('string'))

a1 = dfnew.filter(col('date_range_start').startswith("2020-10"))
a2 = dfnew.filter(col('date_range_start').startswith("2019-10"))
a3 = dfnew.filter(col('date_range_end').startswith("2020-03"))
a4 = dfnew.filter(col('date_range_end').startswith("2019-03"))
a5 = dfnew.filter(col('date_range_start').startswith("2020-10")|col('date_range_start').startswith("2019-10")\
                  |col('date_range_end').startswith("2019-03")|col('date_range_end').startswith("2020-03"))

cbgSet = set(dfcbg1['cbg_fips'].values.tolist())

laDict = dict(zip(dfcbg1['cbg_fips'], dfcbg1['latitude']))
loDict = dict(zip(dfcbg1['cbg_fips'], dfcbg1['longitude']))


ny_state = pyproj.Proj(init="EPSG:2263", preserve_units=True)
def getDis(la1, lo1, la2, lo2):
  lo1p, la1p = ny_state(lo1, la1)[0],ny_state(lo1, la1)[1]
  lo2p, la2p = ny_state(lo2, la2)[0],ny_state(lo2, la2)[1]
  return math.sqrt(pow(la1p - la2p,2) + pow(lo1p - lo2p,2))/5280

def calDis(cbgs, cbgh):
  cbgs = int(cbgs)
  cbgh = int(cbgh)
  las = laDict[cbgs]
  los = loDict[cbgs]
  lah = laDict[cbgh]
  loh = loDict[cbgh]
  return getDis(las, los, lah, loh)

def expd(visitor_home_cbgs, cbg_fips):
  temp = 0
  count = 0
  visitor_home_cbgs = eval(visitor_home_cbgs)
  for x in visitor_home_cbgs:
    x = int(x)
    if(x in cbgSet):
      count = count + visitor_home_cbgs[str(x)]
      temp = temp + calDis(cbg_fips, x)* visitor_home_cbgs[str(x)]
    else: pass
  if(count != 0 and temp != 0):
    return temp/count
  elif(count != 0 and temp == 0):
    return 0.0
  else: 
    return 0

udfExpand = F.udf(expd, FloatType())
dfB = a5.withColumn('dis',udfExpand(a5['visitor_home_cbgs'],a5['poi_cbg']))
dfB = dfB.distinct()
def getTime(date_range_start,date_range_end):
  if(date_range_start.startswith("2020-10") | date_range_end.startswith("2020-10")):
    return "2020-10"
  if(date_range_start.startswith("2019-10") | date_range_end.startswith("2019-10")):
    return "2019-10"
  if(date_range_start.startswith("2020-03") | date_range_end.startswith("2020-03")):
    return "2020-03"
  if(date_range_start.startswith("2019-03") | date_range_end.startswith("2019-03")):
    return "2019-03"
  exp1 = F.udf(getTime, StringType())
dfe = dfB.withColumn('time',exp1(a5['date_range_start'],a5['date_range_end']))
dff = dfe.groupby("poi_cbg").agg(F.collect_list("dis"),F.collect_list("time"))

def fina(dis ,time):
  dic = {'2019-03':0.0,'2020-03':0.0,'2019-10':0.0,'2020-10':0.0}
  n = 0
  while(n < len(dis)):
    dic[time[n]] = dis[n]
    n = n+1
  return [dic['2019-03'],dic['2019-10'],dic['2020-03'],dic['2020-10']]

test_udf = F.udf(fina, ArrayType(FloatType()))
dfg = dff.withColumn('disl',test_udf(dff['collect_list(dis)'],dff['collect_list(time)']))
dfh = dfg.select("poi_cbg", dfg.disl[0].alias("2019-03"), dfg.disl[1].alias("2019-10"), dfg.disl[2].alias("2020-03"), dfg.disl[2].alias("2020-10"))
dfh.rdd.saveAsTextFile(sys.argv[1])














