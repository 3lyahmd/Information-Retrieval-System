
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

    for filename in required_outputs:

        file_path = os.path.join(output_path, filename)

        if not os.path.exists(file_path):
            return False

        if os.path.getsize(file_path) == 0:
            return False

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


# 8. Create Excel Workbook
def combine_all_results():

    from openpyxl import Workbook
    from openpyxl.styles import (
        Font,
        PatternFill,
        Alignment,
        Border,
        Side
    )
    from openpyxl.utils import get_column_letter

    combined_file = os.path.join(
        output_path, "all_results.xlsx"
    )

    workbook = Workbook()

    # Remove default sheet
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    sheet_names = {
        "positional_index.csv": "Positional Index",
        "term_statistics.csv": "Term Statistics",
        "term_statistics_summary.csv": "Statistics Summary",
        "tfidf.csv": "TF-IDF",
        "normalized_tfidf.csv": "Normalized TF-IDF",
        "phrase_queries.csv": "Phrase Queries",
        "similarity.csv": "Similarity",
        "boolean_queries.csv": "Boolean Queries"
    }

    # Header formatting
    header_fill = PatternFill(
        fill_type="solid",
        fgColor="1F4E78"
    )

    header_font = Font(
        bold=True,
        color="FFFFFF",
        size=11
    )

    thin_border = Border(
        bottom=Side(
            style="thin",
            color="D9E2F3"
        )
    )

    # Create sheets
    for filename, sheet_name in sheet_names.items():

        file_path = os.path.join(output_path, filename)

        sheet = workbook.create_sheet(title=sheet_name)

        with open(
            file_path,
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            reader = csv.reader(file)

            for row_index, row in enumerate(reader, start=1):

                for col_index, value in enumerate(row, start=1):

                    cell = sheet.cell(
                        row=row_index,
                        column=col_index,
                        value=value
                    )

                    cell.alignment = Alignment(
                        horizontal="center",
                        vertical="center"
                    )

                    if row_index == 1:

                        cell.fill = header_fill
                        cell.font = header_font

                    else:

                        cell.border = thin_border

        # Freeze header
        sheet.freeze_panes = "A2"

        # Add filters
        sheet.auto_filter.ref = sheet.dimensions

        # Adjust column widths
        for column in sheet.columns:

            max_length = 0

            column_letter = get_column_letter(
                column[0].column
            )

            for cell in column:

                if cell.value is not None:

                    max_length = max(
                        max_length,
                        len(str(cell.value))
                    )

            sheet.column_dimensions[column_letter].width = min(
                max_length + 4,
                40
            )

        sheet.row_dimensions[1].height = 25

    # Save workbook
    workbook.save(combined_file)

    print("\n" + "=" * 60)
    print("EXCEL WORKBOOK CREATED SUCCESSFULLY")
    print("=" * 60)

    print(f"File: {combined_file}")
    print(f"Total Sheets: {len(sheet_names)}")


# 9. Project Header
print("\n" + "=" * 60)
print("       INFORMATION RETRIEVAL PROJECT")
print("=" * 60, flush=True)


# 10. Use Cache or Run Spark
if cache_is_valid():

    print("\nCACHE FOUND!")
    print("Dataset and scripts have not changed.")
    print("Loading saved results without running Spark...\n")

    display_saved_results()

else:

    print("\nCACHE NOT FOUND OR PROJECT CHANGED.")
    print("Running all project tasks...\n", flush=True)

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


    # 11. Verify CSV Files
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


    # 12. Save Cache Signature
    current_signature = calculate_signature()

    with open(cache_file, "w", encoding="utf-8") as file:
        file.write(current_signature)

    print("\nAll CSV files were generated successfully.")
    print("Cache signature saved.")


# 13. Create Excel Workbook
combine_all_results()


# 14. Finish
print("\n" + "=" * 60)
print("       PROJECT COMPLETED SUCCESSFULLY")
print("=" * 60)
