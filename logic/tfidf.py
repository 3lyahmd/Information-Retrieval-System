import os
import csv
import math
from pyspark.sql import SparkSession


#paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataset_path = os.path.join(BASE_DIR, "data set")
output_path = os.path.join(BASE_DIR, "output")

os.makedirs(output_path, exist_ok=True)


#build spark session
spark = SparkSession.builder \
    .appName("IR Project - TF-IDF") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


#read documents
documents = []

for filename in os.listdir(dataset_path):

    if filename.endswith(".txt"):

        file_path = os.path.join(dataset_path, filename)

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read().lower()

        doc_id = filename.replace(".txt", "")
        documents.append((doc_id, content))


#TF
documents_rdd = spark.sparkContext.parallelize(documents)

def calculate_tf(document):

    doc_id, content = document
    words = content.split()

    return [((word, doc_id), 1) for word in words]


tf_rdd = documents_rdd.flatMap(calculate_tf) \
    .reduceByKey(lambda a, b: a + b)


#DF
df_rdd = tf_rdd.map(
    lambda item: (item[0][0], 1)
).reduceByKey(lambda a, b: a + b)


#IDF
N = len(documents)

idf_dict = dict(
    df_rdd.map(
        lambda item: (
            item[0],
            math.log10(N / item[1])
        )
    ).collect()
)


#TF-IDF
tf_results = tf_rdd.collect()

tfidf_matrix = {}

for (word, doc_id), tf in tf_results:

    weighted_tf = 1 + math.log10(tf)
    tfidf = weighted_tf * idf_dict[word]

    if doc_id not in tfidf_matrix:
        tfidf_matrix[doc_id] = {}

    tfidf_matrix[doc_id][word] = tfidf


#print TF-IDF
print("\n========== TF-IDF MATRIX ==========\n")

for doc_id in sorted(tfidf_matrix, key=lambda x: int(x[1:])):

    print(f"{doc_id}:")

    for word in sorted(tfidf_matrix[doc_id]):
        print(f"  {word}: {tfidf_matrix[doc_id][word]:.4f}")


#save TF-IDF to CSV
tfidf_file = os.path.join(output_path, "tfidf.csv")

with open(tfidf_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)
    writer.writerow(["Document", "Term", "TF-IDF"])

    for doc_id in sorted(tfidf_matrix, key=lambda x: int(x[1:])):

        for word in sorted(tfidf_matrix[doc_id]):
            writer.writerow([
                doc_id,
                word,
                f"{tfidf_matrix[doc_id][word]:.6f}"
            ])


#calculate lengths
vector_lengths = {}

for doc_id, weights in tfidf_matrix.items():

    vector_lengths[doc_id] = math.sqrt(
        sum(value ** 2 for value in weights.values())
    )


print("\n========== VECTOR LENGTHS ==========\n")

for doc_id in sorted(vector_lengths, key=lambda x: int(x[1:])):
    print(f"{doc_id}: {vector_lengths[doc_id]:.4f}")


#normalization
normalized_tfidf = {}

for doc_id, weights in tfidf_matrix.items():

    vector_length = vector_lengths[doc_id]

    normalized_tfidf[doc_id] = {
        word: value / vector_length
        for word, value in weights.items()
    }


print("\n========== NORMALIZED TF-IDF ==========\n")

for doc_id in sorted(normalized_tfidf, key=lambda x: int(x[1:])):

    print(f"{doc_id}:")

    for word in sorted(normalized_tfidf[doc_id]):
        print(f"  {word}: {normalized_tfidf[doc_id][word]:.4f}")


#save normalized TF-IDF to CSV
normalized_file = os.path.join(output_path, "normalized_tfidf.csv")

with open(normalized_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)
    writer.writerow(["Document", "Term", "Normalized TF-IDF"])

    for doc_id in sorted(normalized_tfidf, key=lambda x: int(x[1:])):

        for word in sorted(normalized_tfidf[doc_id]):
            writer.writerow([
                doc_id,
                word,
                f"{normalized_tfidf[doc_id][word]:.6f}"
            ])


print(f"\nTF-IDF saved to: {tfidf_file}")
print(f"Normalized TF-IDF saved to: {normalized_file}")

spark.stop()