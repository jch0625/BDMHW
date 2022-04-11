from pyspark import SparkContext

def main(sc):
	
   	KFP = '/tmp/bdm/keyfood_products.csv'
    	dffkp = spark.read.load(KFP, format='csv',header = True, inferSchema = True)
    
    	outputTask1 = ddffkp
	rdd=outputTask1.rdd

	sc.saveAsTextFile(rdd)
if __name__ == "__main__":
	sc = SparkContext()
	main(sc)
