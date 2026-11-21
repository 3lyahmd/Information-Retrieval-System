import os
from pyspark.sql import SparkSession


# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("IR Project - Positional Index") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# 2. Dataset folder path
dataset_path = "data set"


# 3. Read all documents
documents = []

for filename in os.listdir(dataset_path):

    if filename.endswith(".txt"):

        file_path = os.path.join(dataset_path, filename)

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read().lower()

        documents.append((filename.replace(".txt", ""), content))


# 4. Convert documents to RDD
documents_rdd = spark.sparkContext.parallelize(documents)


# 5. Create positional index
def create_positional_index(document):

    doc_id, content = document

    words = content.split()

    result = []

    for position, word in enumerate(words, start=1):
        result.append((word, (doc_id, position)))

    return result


# 6. Generate index using Spark
index_rdd = documents_rdd.flatMap(create_positional_index)


# 7. Group positions by term and document
grouped_index = index_rdd \
    .groupByKey() \
    .mapValues(list) \
    .collect()


# 8. Display positional index
positional_index = {}

for word, occurrences in grouped_index:

    positional_index[word] = {}

    for doc_id, position in occurrences:

        if doc_id not in positional_index[word]:
            positional_index[word][doc_id] = []

        positional_index[word][doc_id].append(position)


# 9. Print results
print("\n========== POSITIONAL INDEX ==========\n")

for word in sorted(positional_index):

    print(f"Term: {word}")

    for doc_id in sorted(positional_index[word]):
        positions = sorted(positional_index[word][doc_id])

        print(f"    {doc_id}: {positions}")

    print()


spark.stop()