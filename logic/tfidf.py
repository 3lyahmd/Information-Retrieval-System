
import os
import sys
import csv
import math

# Add project directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logic.functions import (
    get_paths,
    create_spark_session,
    read_documents
)


# Paths
BASE_DIR, dataset_path, output_path = get_paths()


# Create Spark Session
spark = create_spark_session("IR Project - TF-IDF")


# Read Documents
documents = read_documents(dataset_path)


# TF
documents_rdd = spark.sparkContext.parallelize(documents)


def calculate_tf(document):

    doc_id, content = document
    words = content.split()

    return [((word, doc_id), 1) for word in words]


tf_rdd = documents_rdd.flatMap(calculate_tf) \
    .reduceByKey(lambda a, b: a + b)


# DF
df_rdd = tf_rdd.map(
    lambda item: (item[0][0], 1)
).reduceByKey(lambda a, b: a + b)


# IDF
N = len(documents)

idf_dict = dict(
    df_rdd.map(
        lambda item: (
            item[0],
            math.log10(N / item[1])
        )
    ).collect()
)


# TF-IDF
tf_results = tf_rdd.collect()

tfidf_matrix = {}

for (word, doc_id), tf in tf_results:

    weighted_tf = 1 + math.log10(tf)
    tfidf = weighted_tf * idf_dict[word]

    if doc_id not in tfidf_matrix:
        tfidf_matrix[doc_id] = {}

    tfidf_matrix[doc_id][word] = tfidf


# Print TF-IDF
print("\n========== TF-IDF MATRIX ==========\n")

for doc_id in sorted(tfidf_matrix, key=lambda x: int(x[1:])):

    print(f"{doc_id}:")

    for word in sorted(tfidf_matrix[doc_id]):
        print(f"  {word}: {tfidf_matrix[doc_id][word]:.4f}")


# Save TF-IDF to CSV
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


# Calculate Vector Lengths
vector_lengths = {}

for doc_id, weights in tfidf_matrix.items():

    vector_lengths[doc_id] = math.sqrt(
        sum(value ** 2 for value in weights.values())
    )


print("\n========== VECTOR LENGTHS ==========\n")

for doc_id in sorted(vector_lengths, key=lambda x: int(x[1:])):
    print(f"{doc_id}: {vector_lengths[doc_id]:.4f}")


# Normalization
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


# Save Normalized TF-IDF to CSV
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