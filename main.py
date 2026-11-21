
import os
import sys
import csv
import hashlib
import subprocess


# 1. Project Paths
project_path = os.path.dirname(os.path.abspath(__file__))

dataset_path = os.path.join(project_path, "data set")
output_path = os.path.join(project_path, "output")

os.makedirs(output_path, exist_ok=True)


# 2. Scripts to Run
scripts = [
    "logic/positional_index.py",
    "logic/term_statistics.py",
    "logic/tfidf.py",
    "logic/phrase_queries.py",
    "logic/similarity.py",
    "logic/boolean_queries.py"
]


# 3. Expected CSV Files
required_outputs = [
    "positional_index.csv",
    "term_statistics.csv",
    "term_statistics_summary.csv",
    "tfidf.csv",
    "normalized_tfidf.csv",
    "phrase_queries.csv",
    "similarity.csv",
    "boolean_queries.csv"
]


# 4. Calculate Project Signature
def calculate_signature():

    signature = hashlib.sha256()

    files_to_check = []

    # Dataset files
    for filename in os.listdir(dataset_path):

        if filename.endswith(".txt"):

            files_to_check.append(
                os.path.join(dataset_path, filename)
            )

    # Logic files
    for script in scripts:

        files_to_check.append(
            os.path.join(project_path, script)
        )

    # Sort files for consistent signature
    files_to_check.sort()

    for file_path in files_to_check:

        relative_path = os.path.relpath(
            file_path, project_path
        )

        signature.update(relative_path.encode("utf-8"))

        with open(file_path, "rb") as file:
            signature.update(file.read())

    return signature.hexdigest()


# 5. Cache File
cache_file = os.path.join(
    output_path, "cache_signature.txt"
)


# 6. Check Cache
def cache_is_valid():

    if not os.path.exists(cache_file):
        return False

    # Check CSV files
    for filename in required_outputs:

        file_path = os.path.join(output_path, filename)

        if not os.path.exists(file_path):
            return False

        if os.path.getsize(file_path) == 0:
            return False

    # Compare signatures
    with open(cache_file, "r", encoding="utf-8") as file:
        saved_signature = file.read().strip()

    current_signature = calculate_signature()

    return saved_signature == current_signature


# 7. Display Saved CSV Results
def display_saved_results():

    for filename in required_outputs:

        file_path = os.path.join(output_path, filename)

        print("\n" + "=" * 70)
        print(f"FILE: {filename}")
        print("=" * 70)

        with open(
            file_path,
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            reader = csv.reader(file)

            for row in reader:
                print(" | ".join(row))


# 8. Project Header
print("\n" + "=" * 60)
print("       INFORMATION RETRIEVAL PROJECT")
print("=" * 60, flush=True)


# 9. Use Cache or Run Spark
if cache_is_valid():

    print("\nCACHE FOUND!")
    print("Dataset and scripts have not changed.")
    print("Loading saved results without running Spark...\n")

    display_saved_results()


else:

    print("\nCACHE NOT FOUND OR PROJECT CHANGED.")
    print("Running all project tasks...\n", flush=True)

    # Run all scripts
    for index, script in enumerate(scripts, start=1):

        script_path = os.path.join(project_path, script)

        print(f"\n{'=' * 60}")
        print(f"STEP {index}: Running {script}")
        print("=" * 60, flush=True)

        result = subprocess.run(
            [sys.executable, "-u", script_path],
            cwd=project_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace"
        )

        unwanted_messages = [
            "SUCCESS: The process with PID",
            "WARNING: Using incubator modules",
            "WARNING: package sun.security.action",
            "UserWarning: Please install psutil",
            "Using Spark's default log4j profile",
            "Setting default log level to",
            "To adjust logging level use sc.setLogLevel"
        ]

        # Display script output
        for line in result.stdout.splitlines():

            if not any(
                msg in line for msg in unwanted_messages
            ):
                print(line)

        # Stop if script fails
        if result.returncode != 0:

            print(f"\nERROR: {script} failed!")
            print("Cache signature was not updated.")

            sys.exit(result.returncode)

        print(f"\nFinished: {script}", flush=True)


    # 10. Verify CSV Files
    missing_outputs = []

    for filename in required_outputs:

        file_path = os.path.join(output_path, filename)

        if (
            not os.path.exists(file_path)
            or os.path.getsize(file_path) == 0
        ):
            missing_outputs.append(filename)

    if missing_outputs:

        print("\nERROR: Some CSV files are missing or empty:")

        for filename in missing_outputs:
            print(f"- {filename}")

        print("\nCache signature was not saved.")

        sys.exit(1)


    # 11. Save Cache Signature
    current_signature = calculate_signature()

    with open(cache_file, "w", encoding="utf-8") as file:
        file.write(current_signature)

    print("\nAll CSV files were generated successfully.")
    print("Cache signature saved.")


# 12. Finish
print("\n" + "=" * 60)
print("       PROJECT COMPLETED SUCCESSFULLY")
print("=" * 60)
