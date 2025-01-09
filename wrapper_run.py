import sys
import unittest

def run_tests_for_affected_files(test_files):
    """
    Executes unit tests for a list of test files.
    """
    all_tests = unittest.TestSuite()
    loader = unittest.TestLoader()

    for test_file in test_files:
        try:
            # Dynamically load tests from the specified file
            tests = loader.discover(start_dir='.', pattern=test_file)
            all_tests.addTests(tests)
        except Exception as e:
            print(f"Failed to load tests from {test_file}: {e}")
            sys.exit(1)

    # Run all collected tests
    runner = unittest.TextTestRunner()
    result = runner.run(all_tests)

    # Fail the script if any test fails
    if not result.wasSuccessful():
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
