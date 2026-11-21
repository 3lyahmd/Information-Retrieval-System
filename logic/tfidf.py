import os
import math
from pyspark.sql import SparkSession


# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("IR Project - TF-IDF") \
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

        doc_id = filename.replace(".txt", "")
        documents.append((doc_id, content))


# 4. Create RDD
documents_rdd = spark.sparkContext.parallelize(documents)


# 5. Calculate TF
def calculate_tf(document):

    doc_id, content = document

    words = content.split()

    return [((word, doc_id), 1) for word in words]


tf_rdd = documents_rdd.flatMap(calculate_tf) \
    .reduceByKey(lambda a, b: a + b)


# 6. Calculate DF
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

idf_dict = dict(idf_rdd.collect())


# 8. Calculate Weighted TF and TF-IDF
tf_results = tf_rdd.collect()

tfidf_matrix = {}

for (word, doc_id), tf in tf_results:

    weighted_tf = 1 + math.log10(tf)

    idf = idf_dict[word]

    tfidf = weighted_tf * idf

    if doc_id not in tfidf_matrix:
        tfidf_matrix[doc_id] = {}

    tfidf_matrix[doc_id][word] = tfidf


# 9. Display TF-IDF Matrix
all_terms = sorted(idf_dict.keys())
all_docs = sorted(tfidf_matrix.keys())

print("\n========== TF-IDF MATRIX ==========\n")

print(f"{'Document':<12}", end="")

for term in all_terms:
    print(f"{term:<15}", end="")

print()

for doc_id in all_docs:

    print(f"{doc_id:<12}", end="")

    for term in all_terms:

        value = tfidf_matrix[doc_id].get(term, 0)

        print(f"{value:<15.4f}", end="")

    print()


# 10. Calculate Vector Lengths
vector_lengths = {}

for doc_id in all_docs:

    squared_sum = sum(
        value ** 2
        for value in tfidf_matrix[doc_id].values()
    )

    vector_lengths[doc_id] = math.sqrt(squared_sum)


print("\n========== DOCUMENT VECTOR LENGTHS ==========\n")

for doc_id in all_docs:
    print(f"{doc_id}: {vector_lengths[doc_id]:.4f}")

# 11. Normalize TF-IDF Matrix

normalized_tfidf = {}

for doc_id in all_docs:

    normalized_tfidf[doc_id] = {}

    for term in all_terms:

        tfidf_value = tfidf_matrix[doc_id].get(term, 0)

        normalized_value = tfidf_value / vector_lengths[doc_id]

        normalized_tfidf[doc_id][term] = normalized_value


# 12. Display Normalized TF-IDF Matrix

print("\n========== NORMALIZED TF-IDF MATRIX ==========\n")

print(f"{'Document':<12}", end="")

for term in all_terms:
    print(f"{term:<15}", end="")

print()

for doc_id in all_docs:

    print(f"{doc_id:<12}", end="")

    for term in all_terms:

        value = normalized_tfidf[doc_id][term]

        print(f"{value:<15.4f}", end="")

    print()


# 13. Verify Normalized Vector Lengths

print("\n========== NORMALIZED VECTOR LENGTHS ==========\n")

for doc_id in all_docs:

    squared_sum = sum(
        value ** 2
        for value in normalized_tfidf[doc_id].values()
    )

    normalized_length = math.sqrt(squared_sum)

    print(f"{doc_id}: {normalized_length:.4f}")

spark.stop()