
import os
import sys
import subprocess


project_path = os.path.dirname(os.path.abspath(__file__))

scripts = [
    "logic/positional_index.py",
    "logic/term_statistics.py",
    "logic/tfidf.py",
    "logic/phrase_queries.py",
    "logic/similarity.py",
    "logic/boolean_queries.py"
]

print("\n" + "=" * 60)
print("       INFORMATION RETRIEVAL PROJECT")
print("=" * 60, flush=True)


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

    for line in result.stdout.splitlines():
        if not any(msg in line for msg in unwanted_messages):
            print(line)

    if result.returncode != 0:
        print(f"\nERROR: {script} failed!")
        sys.exit(result.returncode)

    print(f"\nFinished: {script}", flush=True)


print("\n" + "=" * 60)
print("ALL PROJECT TASKS COMPLETED SUCCESSFULLY")
print("=" * 60)
