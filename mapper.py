import json
import subprocess
import sys
import argparse
import os

def get_changed_files():
    """
    Get the list of changed files between the last two commits.
    """
    try:
        # Fetch changes from the last two commits
        result = subprocess.run(
            ["git", "diff", "--name-only", "HEAD~1", "HEAD"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return result.stdout.strip().split("\n")
    except subprocess.CalledProcessError as e:
        print(f"Error fetching changed files: {e.stderr}")
        sys.exit(1)

def determine_affected_tests(mapping_file, changed_files):
    """
    Determine affected tests based on the mapping and changed files.
    """
    affected_tests = set()
    try:
        with open(mapping_file, "r") as f:
            mapping = json.load(f)

        for file in changed_files:
            for table, paths in mapping.items():
                if file == paths["config"] or file == paths["test"]:
                    affected_tests.add(paths["test"])

    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading mapping file: {e}")
        sys.exit(1)

    return list(affected_tests)

if __name__ == "__main__":
    # Use argparse to accept the mapping file path as an argument
    parser = argparse.ArgumentParser(description="Determine affected test cases.")
    parser.add_argument(
        "--mapping-file",
        type=str,
        help="Path to the Test_Cases_Mapping.json file. Can also be set via the MAPPING_FILE_PATH environment variable.",
    )
    args = parser.parse_args()

    # Check if mapping file is provided via argument or environment variable
    mapping_file = args.mapping_file or os.getenv("MAPPING_FILE_PATH")
    if not mapping_file:
        print("Error: The mapping file path must be provided either via --mapping-file argument or MAPPING_FILE_PATH environment variable.")
        sys.exit(1)

    # Get the list of changed files
    changed_files = get_changed_files()
    print("Changed files:", changed_files)

    # Determine affected tests
    affected_tests = determine_affected_tests(mapping_file, changed_files)
    print("Affected tests:", affected_tests)

    # Output affected tests to a file (optional)
    with open("affected_tests.txt", "w") as f:
        for test in affected_tests:
            f.write(test + "\n")

    # Exit with success if tests were found
    if affected_tests:
        sys.exit(0)
    else:
        print("No affected tests found.")
        sys.exit(1)
