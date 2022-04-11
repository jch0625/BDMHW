from pyspark import SparkContext

def main(sc):
	
   	KFP = '/tmp/bdm/keyfood_products.csv'
    	dffkp = spark.read.load(KFP, format='csv',header = True, inferSchema = True)
    
    	outputTask1 = ddffkp
	sc.textFile(outputTask1)
if __name__ == "__main__":
	sc = SparkContext()
	main(sc)
