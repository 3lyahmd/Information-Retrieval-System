
import os
import math
from pyspark.sql import SparkSession


# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("IR Project - Similarity") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# 2. Read Documents
dataset_path = "data set"

documents = []

for filename in os.listdir(dataset_path):

    if filename.endswith(".txt"):

        file_path = os.path.join(dataset_path, filename)

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read().lower()

        doc_id = filename.replace(".txt", "")
        documents.append((doc_id, content))


# 3. Calculate TF
documents_rdd = spark.sparkContext.parallelize(documents)

def calculate_tf(document):

    doc_id, content = document

    words = content.split()

    return [((word, doc_id), 1) for word in words]


tf_rdd = documents_rdd.flatMap(calculate_tf) \
    .reduceByKey(lambda a, b: a + b)


# 4. Calculate DF
df_rdd = tf_rdd.map(
    lambda item: (item[0][0], 1)
).reduceByKey(lambda a, b: a + b)


# 5. Calculate IDF
N = len(documents)

idf_dict = dict(
    df_rdd.map(
        lambda item: (
            item[0],
            math.log10(N / item[1])
        )
    ).collect()
)


# 6. Calculate TF-IDF
tfidf_matrix = {}

for (word, doc_id), tf in tf_rdd.collect():

    weighted_tf = 1 + math.log10(tf)

    tfidf = weighted_tf * idf_dict[word]

    if doc_id not in tfidf_matrix:
        tfidf_matrix[doc_id] = {}

    tfidf_matrix[doc_id][word] = tfidf


# 7. Normalize Document Vectors
normalized_docs = {}

for doc_id, weights in tfidf_matrix.items():

    vector_length = math.sqrt(
        sum(value ** 2 for value in weights.values())
    )

    normalized_docs[doc_id] = {
        word: value / vector_length
        for word, value in weights.items()
    }


# 8. Calculate Query Similarity
def calculate_similarity(query):

    query_terms = query.lower().split()

    query_tf = {}

    for term in query_terms:
        query_tf[term] = query_tf.get(term, 0) + 1

    query_vector = {}

    for term, tf in query_tf.items():

        if term in idf_dict:

            weighted_tf = 1 + math.log10(tf)

            query_vector[term] = weighted_tf * idf_dict[term]

    query_length = math.sqrt(
        sum(value ** 2 for value in query_vector.values())
    )

    if query_length == 0:
        return []

    normalized_query = {
        term: value / query_length
        for term, value in query_vector.items()
    }

    scores = []

    for doc_id, doc_vector in normalized_docs.items():

        score = sum(
            normalized_query.get(term, 0) * doc_vector.get(term, 0)
            for term in normalized_query
        )

        scores.append((doc_id, score))

    return sorted(scores, key=lambda item: item[1], reverse=True)


# 9. Test Similarity
queries = [
    "machine learning",
    "data science"
]

for query in queries:

    print(f"\n========== SIMILARITY: {query} ==========\n")

    results = calculate_similarity(query)

    for doc_id, score in results:
        print(f"{doc_id}: {score:.4f}")


spark.stop()
