
import os
import sys
import csv

sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from logic.functions import (
    get_paths,
    create_spark_session,
    read_documents
)

BASE_DIR, dataset_path, output_path = get_paths()

spark = create_spark_session("IR Project - Positional Index")

documents = read_documents(dataset_path)

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

positional_index = {}

for word, postings in index_data:

    positional_index[word] = {}

    for doc_id, position in postings:

        if doc_id not in positional_index[word]:
            positional_index[word][doc_id] = []

        positional_index[word][doc_id].append(position)


# Sort documents and positions
for word in positional_index:

    positional_index[word] = dict(
        sorted(
            positional_index[word].items(),
            key=lambda item: int(item[0][1:])
        )
    )

    for doc_id in positional_index[word]:
        positional_index[word][doc_id].sort()


# Prepare formatted data
table_data = []

for word in sorted(positional_index):

    postings_text = "; ".join(
        f"{doc_id}: {', '.join(map(str, positions))}"
        for doc_id, positions in positional_index[word].items()
    )

    table_data.append((word, postings_text))


# Print table
print("\n========== POSITIONAL INDEX ==========\n")

print(f"{'Term':<20} | Positional Index")
print("-" * 100)

for word, postings_text in table_data:

    print(f"{word:<20} | {postings_text}")


# Save CSV
csv_file = os.path.join(output_path, "positional_index.csv")

with open(csv_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)

    writer.writerow(["Term", "Positional Index"])

    writer.writerows(table_data)


print("\nTotal Terms:", len(table_data))
print("Positional Index saved to:", csv_file)

spark.stop()