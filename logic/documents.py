
import os
import sys
import csv
import textwrap

sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from logic.functions import get_paths, read_documents


BASE_DIR, dataset_path, output_path = get_paths()


def generate_documents_csv():

    print("\n========== DOCUMENTS TABLE ==========\n")

    documents = read_documents(dataset_path)

    output_file = os.path.join(output_path, "documents.csv")

    table_data = []

    for doc_id, content in documents:

        document_text = content.strip()
        word_count = len(document_text.split())

        table_data.append([
            doc_id,
            document_text,
            word_count
        ])

    # Save CSV
    with open(output_file, "w", newline="", encoding="utf-8-sig") as file:

        writer = csv.writer(file)

        writer.writerow([
            "File",
            "Exact document text",
            "Word count"
        ])

        writer.writerows(table_data)

    # Print formatted table
    file_width = 10
    text_width = 75
    count_width = 12

    border = (
        "+"
        + "-" * (file_width + 2)
        + "+"
        + "-" * (text_width + 2)
        + "+"
        + "-" * (count_width + 2)
        + "+"
    )

    print(border)

    print(
        f"| {'File':<{file_width}} "
        f"| {'Exact document text':<{text_width}} "
        f"| {'Word count':<{count_width}} |"
    )

    print(border)

    for doc_id, document_text, word_count in table_data:

        wrapped_text = textwrap.wrap(
            document_text,
            width=text_width
        ) or [""]

        for index, line in enumerate(wrapped_text):

            if index == 0:
                file_cell = doc_id
                count_cell = str(word_count)
            else:
                file_cell = ""
                count_cell = ""

            print(
                f"| {file_cell:<{file_width}} "
                f"| {line:<{text_width}} "
                f"| {count_cell:<{count_width}} |"
            )

        print(border)

    print("\nTotal Documents:", len(table_data))
    print("CSV Saved Successfully:", output_file)


if __name__ == "__main__":

    generate_documents_csv()