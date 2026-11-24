
import os
import sys
from pyspark.sql import SparkSession


def get_paths(dataset_path=None):

    BASE_DIR = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )

    if dataset_path is None:
        dataset_path = os.environ.get("IR_DATASET_PATH")

    if not dataset_path:
        dataset_path = os.path.join(BASE_DIR, "data set")

    dataset_path = os.path.abspath(dataset_path)

    if not os.path.isdir(dataset_path):
        raise FileNotFoundError(
            f"Dataset folder not found: {dataset_path}"
        )

    output_path = os.path.join(BASE_DIR, "output")

    os.makedirs(output_path, exist_ok=True)

    return BASE_DIR, dataset_path, output_path

def create_spark_session(app_name):

    # Use the project's virtual environment for Spark workers
    python_path = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def read_documents(dataset_path):

    documents = []

    for filename in os.listdir(dataset_path):

        if filename.endswith(".txt"):

            file_path = os.path.join(dataset_path, filename)

            with open(file_path, "r", encoding="utf-8") as file:
                content = file.read().lower()

            doc_id = filename.replace(".txt", "")
            documents.append((doc_id, content))

    return documents
