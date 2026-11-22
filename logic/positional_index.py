
import os
import csv
from pyspark.sql import SparkSession


#paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataset_path = os.path.join(BASE_DIR, "data set")
output_path = os.path.join(BASE_DIR, "output")

os.makedirs(output_path, exist_ok=True)


#build spark session
spark = SparkSession.builder \
    .appName("IR Project - Positional Index") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


#read documents
documents = []

for filename in os.listdir(dataset_path):

    if filename.endswith(".txt"):

        file_path = os.path.join(dataset_path, filename)

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read().lower() #tokinization

        doc_id = filename.replace(".txt", "")
        documents.append((doc_id, content))


#build positional index
documents_rdd = spark.sparkContext.parallelize(documents)


def create_positional_index(document):

    doc_id, content = document
    words = content.split()

    return [
        (word, (doc_id, position))
        for position, word in enumerate(words, start=1)
    ]


positional_rdd = documents_rdd.flatMap(create_positional_index)

index_rdd = positional_rdd.groupByKey().mapValues(list)

index_data = index_rdd.collect()


# convert to dictionary
positional_index = {}

for word, postings in index_data:

    positional_index[word] = {}

    for doc_id, position in postings:

        if doc_id not in positional_index[word]:
            positional_index[word][doc_id] = []

        positional_index[word][doc_id].append(position)


#printing
print("\n========== POSITIONAL INDEX ==========\n")

for word in sorted(positional_index):

    print(f"{word}:")

    for doc_id in sorted(
        positional_index[word],
        key=lambda x: int(x[1:])
    ):

        positions = sorted(positional_index[word][doc_id])

        print(f"  {doc_id}: {positions}")


#saving to csv
csv_file = os.path.join(output_path, "positional_index.csv")

with open(csv_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)

    writer.writerow(["Term", "Document", "Positions"])

    for word in sorted(positional_index):

        for doc_id in sorted(
            positional_index[word],
            key=lambda x: int(x[1:])
        ):

            positions = sorted(positional_index[word][doc_id])

            writer.writerow([
                word,
                doc_id,
                " ".join(map(str, positions))
            ])


print(f"\nPositional Index saved to: {csv_file}")


spark.stop()