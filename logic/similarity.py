
import os
import sys
import csv
import math

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logic.functions import (
    get_paths,
    create_spark_session,
    read_documents
)


# Paths
BASE_DIR, dataset_path, output_path = get_paths()


# Building Spark Session
spark = create_spark_session("IR Project - Similarity")


# Reading Documents
documents = read_documents(dataset_path)


# TF
documents_rdd = spark.sparkContext.parallelize(documents)

tf_rdd = documents_rdd.flatMap(
    lambda doc: [
        ((word, doc[0]), 1)
        for word in doc[1].split()
    ]
).reduceByKey(lambda a, b: a + b)


# DF
df_rdd = tf_rdd.map(
    lambda item: (item[0][0], 1)
).reduceByKey(lambda a, b: a + b)

N = len(documents)

df_dict = dict(df_rdd.collect())

idf_dict = {
    word: math.log10(N / df)
    for word, df in df_dict.items()
}


# TF-IDF Matrix
tfidf_data = tf_rdd.collect()

tfidf_matrix = {}

for (word, doc_id), tf in tfidf_data:

    weighted_tf = 1 + math.log10(tf)
    idf = idf_dict[word]

    score = weighted_tf * idf

    if doc_id not in tfidf_matrix:
        tfidf_matrix[doc_id] = {}

    tfidf_matrix[doc_id][word] = score


# Normalization
normalized_matrix = {}

for doc_id, vector in tfidf_matrix.items():

    length = math.sqrt(
        sum(value ** 2 for value in vector.values())
    )

    if length == 0:
        normalized_matrix[doc_id] = {}
    else:
        normalized_matrix[doc_id] = {
            word: value / length
            for word, value in vector.items()
        }


# Cosine Similarity
def calculate_similarity(query):

    words = query.lower().split()

    query_tf = {}

    for word in words:
        query_tf[word] = query_tf.get(word, 0) + 1

    query_vector = {}

    for word, tf in query_tf.items():

        if word in idf_dict:

            weighted_tf = 1 + math.log10(tf)
            query_vector[word] = weighted_tf * idf_dict[word]

    query_length = math.sqrt(
        sum(value ** 2 for value in query_vector.values())
    )

    if query_length == 0:
        return []

    normalized_query = {
        word: value / query_length
        for word, value in query_vector.items()
    }

    scores = []

    for doc_id, doc_vector in normalized_matrix.items():

        similarity = sum(
            normalized_query.get(word, 0) * value
            for word, value in doc_vector.items()
        )

        scores.append((doc_id, similarity))

    scores.sort(key=lambda x: x[1], reverse=True)

    return scores


# Testing
queries = [
    "machine learning",
    "data science"
]

all_results = []

for query in queries:

    print(f"\n========== QUERY: {query} ==========\n")

    results = calculate_similarity(query)

    for doc_id, score in results:

        print(f"{doc_id}: {score:.4f}")

        all_results.append([
            query,
            doc_id,
            round(score, 6)
        ])


# CSV
csv_file = os.path.join(output_path, "similarity.csv")

with open(csv_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)

    writer.writerow([
        "Query",
        "Document",
        "Cosine Similarity"
    ])

    writer.writerows(all_results)


print(f"\nSimilarity Results saved to: {csv_file}")


spark.stop()