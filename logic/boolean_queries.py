import os
import csv
import math
from pyspark.sql import SparkSession


#paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataset_path = os.path.join(BASE_DIR, "data set")
output_path = os.path.join(BASE_DIR, "output")

os.makedirs(output_path, exist_ok=True)


#bulding spark session
spark = SparkSession.builder \
    .appName("IR Project - Boolean Queries") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


#rwad documents
documents = {}

for filename in os.listdir(dataset_path):

    if filename.endswith(".txt"):

        file_path = os.path.join(dataset_path, filename)

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read().lower()

        doc_id = filename.replace(".txt", "")
        documents[doc_id] = content.split()


#calculating tf
documents_rdd = spark.sparkContext.parallelize(
    [(doc_id, words) for doc_id, words in documents.items()]
)


def calculate_tf(document):

    doc_id, words = document

    return [((word, doc_id), 1) for word in words]


tf_rdd = documents_rdd.flatMap(calculate_tf) \
    .reduceByKey(lambda a, b: a + b)


#calculating df idf
df_rdd = tf_rdd.map(
    lambda item: (item[0][0], 1)
).reduceByKey(lambda a, b: a + b)

N = len(documents)

idf_dict = dict(
    df_rdd.map(
        lambda item: (
            item[0],
            math.log10(N / item[1])
        )
    ).collect()
)


#calculate tf-idf
tfidf_matrix = {}

for (word, doc_id), tf in tf_rdd.collect():

    weighted_tf = 1 + math.log10(tf)
    tfidf = weighted_tf * idf_dict[word]

    if doc_id not in tfidf_matrix:
        tfidf_matrix[doc_id] = {}

    tfidf_matrix[doc_id][word] = tfidf


#normalization
normalized_docs = {}

for doc_id, weights in tfidf_matrix.items():

    vector_length = math.sqrt(
        sum(value ** 2 for value in weights.values())
    )

    if vector_length == 0:
        normalized_docs[doc_id] = {}
    else:
        normalized_docs[doc_id] = {
            word: value / vector_length
            for word, value in weights.items()
        }


#similarity
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
        return {}

    normalized_query = {
        term: value / query_length
        for term, value in query_vector.items()
    }

    scores = {}

    for doc_id, doc_vector in normalized_docs.items():

        score = sum(
            normalized_query.get(term, 0) *
            doc_vector.get(term, 0)
            for term in normalized_query
        )

        scores[doc_id] = score

    return scores


#boolean query
def contains_all_terms(doc_id, terms):

    words = documents[doc_id]

    return all(term in words for term in terms)


# Query 1
query1_results = []

for doc_id in documents:

    if (
        contains_all_terms(doc_id, ["machine", "learning"])
        and
        contains_all_terms(doc_id, ["data", "science"])
    ):
        query1_results.append(doc_id)


# Query 2
query2_results = []

for doc_id in documents:

    has_machine_learning = contains_all_terms(
        doc_id, ["machine", "learning"]
    )

    has_data_science = contains_all_terms(
        doc_id, ["data", "science"]
    )

    if has_machine_learning and not has_data_science:
        query2_results.append(doc_id)


#ranking by similarity
def rank_results(results, query):

    scores = calculate_similarity(query)

    ranked = [
        (doc_id, scores[doc_id])
        for doc_id in results
    ]

    return sorted(
        ranked,
        key=lambda item: item[1],
        reverse=True
    )


# display and collecting results
all_results = []


print("\n========== BOOLEAN QUERY 1 ==========")
print("Query: machine learning AND data science\n")

ranked_query1 = rank_results(
    query1_results,
    "machine learning data science"
)

for doc_id, score in ranked_query1:

    print(f"{doc_id}: Similarity = {score:.4f}")

    all_results.append([
        "machine learning AND data science",
        doc_id,
        round(score, 6)
    ])


print("\n========== BOOLEAN QUERY 2 ==========")
print("Query: machine learning AND NOT data science\n")

ranked_query2 = rank_results(
    query2_results,
    "machine learning"
)

for doc_id, score in ranked_query2:

    print(f"{doc_id}: Similarity = {score:.4f}")

    all_results.append([
        "machine learning AND NOT data science",
        doc_id,
        round(score, 6)
    ])


#saving to CSV
csv_file = os.path.join(output_path, "boolean_queries.csv")

with open(csv_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)

    writer.writerow([
        "Query",
        "Document",
        "Cosine Similarity"
    ])

    writer.writerows(all_results)


print(f"\nBoolean Query Results saved to: {csv_file}")

spark.stop()