
import os
from pyspark.sql import SparkSession


# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("IR Project - Phrase Queries") \
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


# 3. Create RDD
documents_rdd = spark.sparkContext.parallelize(documents)


# 4. Build Positional Index
def get_positions(document):

    doc_id, content = document

    words = content.split()

    result = []

    for position, word in enumerate(words, start=1):
        result.append((word, (doc_id, position)))

    return result


positional_rdd = documents_rdd.flatMap(get_positions)

grouped_index = positional_rdd.groupByKey().mapValues(list).collect()

positional_index = {}

for word, postings in grouped_index:

    positional_index[word] = {}

    for doc_id, position in postings:

        if doc_id not in positional_index[word]:
            positional_index[word][doc_id] = []

        positional_index[word][doc_id].append(position)


# 5. Phrase Search Function
def phrase_search(phrase):

    terms = phrase.lower().split()

    if len(terms) < 2:
        return {}

    first_term = terms[0]

    if first_term not in positional_index:
        return {}

    matches = {}

    for doc_id, positions in positional_index[first_term].items():

        matching_positions = []

        for position in positions:

            found = True

            for i in range(1, len(terms)):

                term = terms[i]

                if term not in positional_index:
                    found = False
                    break

                term_positions = positional_index[term].get(doc_id, [])

                if position + i not in term_positions:
                    found = False
                    break

            if found:
                matching_positions.append(position)

        if matching_positions:
            matches[doc_id] = matching_positions

    return matches


# 6. Test Phrase Queries
queries = [
    "machine learning",
    "data science"
]

for query in queries:

    results = phrase_search(query)

    print(f"\n========== PHRASE QUERY: {query} ==========\n")

    if results:

        for doc_id in sorted(results, key=lambda x: int(x[1:])):
            print(f"{doc_id}: positions {results[doc_id]}")

    else:
        print("No matching documents found.")


spark.stop()
