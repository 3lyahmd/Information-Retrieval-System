
import os
import csv
from pyspark.sql import SparkSession


# 1. Project Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dataset_path = os.path.join(BASE_DIR, "data set")
output_path = os.path.join(BASE_DIR, "output")

os.makedirs(output_path, exist_ok=True)


# 2. Create Spark Session
spark = SparkSession.builder \
    .appName("IR Project - Phrase Queries") \
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


# 4. Build Positional Index
documents_rdd = spark.sparkContext.parallelize(documents)


def get_positions(document):

    doc_id, content = document
    words = content.split()

    return [
        (word, (doc_id, position))
        for position, word in enumerate(words, start=1)
    ]


positional_rdd = documents_rdd.flatMap(get_positions)

index_rdd = positional_rdd.groupByKey().mapValues(list)

index_data = index_rdd.collect()


# 5. Convert to Dictionary
positional_index = {}

for word, postings in index_data:

    positional_index[word] = {}

    for doc_id, position in postings:

        if doc_id not in positional_index[word]:
            positional_index[word][doc_id] = []

        positional_index[word][doc_id].append(position)


# 6. Phrase Search Function
def phrase_search(phrase):

    terms = phrase.lower().split()

    if len(terms) < 2:
        return {}

    if any(term not in positional_index for term in terms):
        return {}

    results = {}

    first_term = terms[0]

    for doc_id, positions in positional_index[first_term].items():

        matching_positions = []

        for position in positions:

            match = True

            for i in range(1, len(terms)):

                next_term = terms[i]

                if (
                    next_term not in positional_index
                    or doc_id not in positional_index[next_term]
                    or position + i not in positional_index[next_term][doc_id]
                ):

                    match = False
                    break

            if match:
                matching_positions.append(position)

        if matching_positions:
            results[doc_id] = matching_positions

    return results


# 7. Test Phrase Queries
queries = [
    "machine learning",
    "data science"
]

all_results = []

for query in queries:

    print(f"\n========== PHRASE QUERY: {query} ==========\n")

    results = phrase_search(query)

    if not results:
        print("No matching documents found.")

    for doc_id in sorted(results, key=lambda x: int(x[1:])):

        positions = sorted(results[doc_id])

        print(f"{doc_id}: {positions}")

        all_results.append([
            query,
            doc_id,
            " ".join(map(str, positions))
        ])


# 8. Save Results to CSV
csv_file = os.path.join(output_path, "phrase_queries.csv")

with open(csv_file, "w", newline="", encoding="utf-8-sig") as file:

    writer = csv.writer(file)

    writer.writerow(["Phrase", "Document", "Positions"])

    writer.writerows(all_results)


print(f"\nPhrase Query Results saved to: {csv_file}")


# 9. Stop Spark
spark.stop()
