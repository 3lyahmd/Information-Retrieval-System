
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
spark = create_spark_session("IR Project - Term Statistics")


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


# Calculate IDF
N = len(documents)

idf_rdd = df_rdd.map(
    lambda item: (
        item[0],
        math.log10(N / item[1])
    )
)


tf_results = tf_rdd.collect()
df_results = df_rdd.collect()
idf_results = idf_rdd.collect()


# Print TF
print("\n========== TERM FREQUENCY (TF) ==========\n")

for (word, doc_id), tf in sorted(tf_results):
    print(f"{word} - {doc_id}: {tf}")


# Print DF
print("\n========== DOCUMENT FREQUENCY (DF) ==========\n")

for word, df in sorted(df_results):
    print(f"{word}: {df}")


# Print IDF
print("\n========== INVERSE DOCUMENT FREQUENCY (IDF) ==========\n")

for word, idf in sorted(idf_results):
    print(f"{word}: {idf:.4f}")


# Save TF to CSV
tf_file = os.path.join(output_path, "term_statistics.csv")

with open(tf_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)
    writer.writerow(["Term", "Document", "TF"])

    for (word, doc_id), tf in sorted(tf_results):
        writer.writerow([word, doc_id, tf])


# Save DF and IDF to CSV
summary_file = os.path.join(output_path, "term_statistics_summary.csv")

df_dict = dict(df_results)
idf_dict = dict(idf_results)

with open(summary_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)
    writer.writerow(["Term", "DF", "IDF"])

    for word in sorted(df_dict):
        writer.writerow([
            word,
            df_dict[word],
            f"{idf_dict[word]:.4f}"
        ])


print(f"\nTF saved to: {tf_file}")
print(f"DF and IDF saved to: {summary_file}")


spark.stop()