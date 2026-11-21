from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IR Project Test") \
    .master("local[*]") \
    .getOrCreate()

print("Spark is working!")
print("Spark Version:", spark.version)

data = [("Ali", 20), ("Ahmed", 21), ("Omar", 22)]

df = spark.createDataFrame(data, ["Name", "Age"])

df.show()

spark.stop()