import subprocess
import sys

def run_tests_for_affected_files(test_files):
    """
    Executes each affected test file as a standalone script.
    """
    for test_file in test_files:
        print(f"Running tests in file: {test_file}")
        try:
            # Execute the test file as a standalone script
            subprocess.run(["python", test_file], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Tests failed in {test_file}: {e}")
            sys.exit(1)

if __name__ == "__main__":
    # Read affected test files from the file
    try:
        with open("affected_tests.txt", "r") as f:
            affected_test_files = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("No affected_tests.txt file found. Exiting.")
        sys.exit(1)

    if not affected_test_files:
        print("No affected tests to run. Exiting.")
        sys.exit(0)

    print(f"Running tests for affected files: {affected_test_files}")
    run_tests_for_affected_files(affected_test_files)
