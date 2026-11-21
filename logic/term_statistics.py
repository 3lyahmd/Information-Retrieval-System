
import os
import csv
import math
from pyspark.sql import SparkSession


# 1. Project Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataset_path = os.path.join(BASE_DIR, "data set")
output_path = os.path.join(BASE_DIR, "output")

os.makedirs(output_path, exist_ok=True)


# 2. Create Spark Session
spark = SparkSession.builder \
    .appName("IR Project - Term Statistics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# 3. Read Documents
documents = []

for filename in os.listdir(dataset_path):

    if filename.endswith(".txt"):

        file_path = os.path.join(dataset_path, filename)

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read().lower()

        doc_id = filename.replace(".txt", "")
        documents.append((doc_id, content))


# 4. Calculate TF
documents_rdd = spark.sparkContext.parallelize(documents)

def calculate_tf(document):

    doc_id, content = document
    words = content.split()

    return [((word, doc_id), 1) for word in words]


tf_rdd = documents_rdd.flatMap(calculate_tf) \
    .reduceByKey(lambda a, b: a + b)


# 5. Calculate DF
df_rdd = tf_rdd.map(
    lambda item: (item[0][0], 1)
).reduceByKey(lambda a, b: a + b)


# 6. Calculate IDF
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


# 7. Print TF
print("\n========== TERM FREQUENCY (TF) ==========\n")

for (word, doc_id), tf in sorted(tf_results):
    print(f"{word} - {doc_id}: {tf}")


# 8. Print DF
print("\n========== DOCUMENT FREQUENCY (DF) ==========\n")

for word, df in sorted(df_results):
    print(f"{word}: {df}")


# 9. Print IDF
print("\n========== INVERSE DOCUMENT FREQUENCY (IDF) ==========\n")

for word, idf in sorted(idf_results):
    print(f"{word}: {idf:.4f}")


# 10. Save TF to CSV
tf_file = os.path.join(output_path, "term_statistics.csv")

with open(tf_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)
    writer.writerow(["Term", "Document", "TF"])

    for (word, doc_id), tf in sorted(tf_results):
        writer.writerow([word, doc_id, tf])


# 11. Save DF and IDF to CSV
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


# 12. Stop Spark
spark.stop()
