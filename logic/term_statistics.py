import os
import math
from pyspark.sql import SparkSession


# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("IR Project - Term Statistics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# 2. Dataset path
dataset_path = "data set"


# 3. Read documents
documents = []

for filename in os.listdir(dataset_path):

    if filename.endswith(".txt"):

        file_path = os.path.join(dataset_path, filename)

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read().lower()

        documents.append((filename.replace(".txt", ""), content))


# 4. Create RDD
documents_rdd = spark.sparkContext.parallelize(documents)


# 5. Calculate Term Frequency (TF)
def calculate_tf(document):

    doc_id, content = document

    words = content.split()

    result = []

    for word in words:
        result.append(((word, doc_id), 1))

    return result


tf_rdd = documents_rdd.flatMap(calculate_tf) \
    .reduceByKey(lambda a, b: a + b)


# 6. Calculate Document Frequency (DF)
df_rdd = tf_rdd.map(
    lambda item: (item[0][0], 1)
).reduceByKey(lambda a, b: a + b)


# 7. Calculate IDF
N = len(documents)

idf_rdd = df_rdd.map(
    lambda item: (
        item[0],
        math.log10(N / item[1])
    )
)


# 8. Collect results
tf_results = tf_rdd.collect()
df_results = df_rdd.collect()
idf_results = idf_rdd.collect()


# 9. Display TF
print("\n========== TERM FREQUENCY (TF) ==========\n")

for (word, doc_id), tf in sorted(tf_results):
    print(f"Term: {word:<15} Document: {doc_id:<5} TF: {tf}")


# 10. Display DF
print("\n========== DOCUMENT FREQUENCY (DF) ==========\n")

for word, df in sorted(df_results):
    print(f"Term: {word:<15} DF: {df}")


# 11. Display IDF
print("\n========== INVERSE DOCUMENT FREQUENCY (IDF) ==========\n")

for word, idf in sorted(idf_results):
    print(f"Term: {word:<15} IDF: {idf:.4f}")


spark.stop()