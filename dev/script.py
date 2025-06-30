
import unittest
import sys
import pandas as pd
import sqlite3
from tests import TestNewData, TestStudentCleaner, TestCoursesCleaner, TestStudentJobCleaner, TestPreMerge, TestSchema
from ETLPipeline import ETLPipeline
import logging
import os

# Get the directory where this script is located
# Build the full path to changelog.md  and csv in the same directory
base_dir = os.path.dirname(os.path.abspath(__file__))
changelog_path = os.path.join(base_dir, "changelog.md")


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
        handlers=[
            #logging.FileHandler("etl_pipeline.log"), if logging of the run is wanted remove #
            logging.StreamHandler(sys.stdout)
        ]
    )
logger = logging.getLogger(__name__)

def run_tests(classes):
    """
    Run all test methods from a list of test classes.

    Parameters:
        classes (list): List of test classes, there are 3: pre_tests, pre_merge_tests and schema_tests

    Returns:
        bool: True if all tests passed, False otherwise.
    """
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    for cls in classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


def main():
    prod_con = None
    # Setup and initialize log
    setup_logging()
    logger.info("Start Log")

    # Check for current version and calculate next version for changelog
    with open(changelog_path, 'r') as f:
        lines = f.readlines()

    if len(lines) == 0:
        next_version = 0
    else:
        first_line = lines[0].strip()  # e.g., '## 0.0.3'
        if first_line.startswith('## 0.0.'):
            patch_num = int(first_line.split('.')[-1])
            next_version = patch_num + 1
        else:
            next_version = 0
    try:
        logger.info('Running pre-ETL tests...')
        pre_tests = [
            TestNewData,
            TestStudentCleaner,
            TestCoursesCleaner,
            TestStudentJobCleaner
        ]
        if not run_tests(pre_tests):
            raise Exception(" Pre-ETL tests failed.")

        # --- Load & Clean Data ---

        logger.info('Running ETL pipeline...')
        pipeline = ETLPipeline('cademycode.db')
        students, careers, jobs = pipeline.load_data()
        cleaned_students, cleaned_careers, cleaned_jobs = pipeline.clean_data(students, careers, jobs)

        # --- Pre-Merge Tests ---
        logger.info('Running pre merge tests...')
        TestPreMerge.cleaned_students = cleaned_students
        TestPreMerge.cleaned_careers = cleaned_careers
        TestPreMerge.cleaned_jobs = cleaned_jobs

        pre_merge_tests = [TestPreMerge]

        if not run_tests(pre_merge_tests):
            raise Exception(" Pre-merge tests failed.")

        # --- Merge Tables ---
        logger.info('merging...')
        final_df = pipeline.merge_cleaned_tables(cleaned_students, cleaned_careers, cleaned_jobs)

        # --- Schema Test ---
        logger.info('Running schema tests...')
        # getting prod dataframe for comparison
        base_dir = os.path.dirname(os.path.abspath(__file__))
        prod_path = os.path.join(base_dir, 'cademycode_ds.db')
        prod_con = sqlite3.connect(prod_path)
        prod_df = pd.read_sql("SELECT * FROM cademycode_aggregated", prod_con)

        # give parameters to testSchema final df and prod df
        TestSchema.final_df = final_df
        TestSchema.prod_df = prod_df

        schema_tests = [TestSchema]
        if not run_tests(schema_tests):
            raise Exception(" Schema test failed.")

        # --- Upsert and Export csv ---
        logger.info('upserting to ds table...')
        pipeline.upsert_to_prod(final_df, prod_con)

        print("\n creating csv...\n")
        logger.info('exporting to csv...')
        pipeline.export_csv(prod_con)


        # --- Create automatic changelog entry ---
        logger.info("Updating changelog...")
        new_missing_data = cleaned_students[
            (cleaned_students['job_id'] == 0) |
            (cleaned_students['num_course_taken'] == 0) |
            (cleaned_students['current_career_path_id'] == 0) |
            (cleaned_students['time_spent_hrs'] == 0)
        ]
        with open(changelog_path, 'r') as f:
            lines = f.readlines()

        new_lines = [
            f'## 0.0.{next_version}\n',
            '### Added\n',
            f'- {len(cleaned_students)} more data to the database of raw data\n',
            f'- {len(new_missing_data)} new missing data to incomplete_data table\n\n'
        ]

        with open(changelog_path, 'w') as f:
            f.writelines(new_lines + lines)

        logger.info(f"Changelog updated to version 0.0.{next_version}")

    except Exception as e:
        logger.exception(f"Failure: {e}")
        print(f" Failure: {e}")
        sys.exit(1)

    finally:
        if prod_con:
            prod_con.close()
            logger.info("Closed production DB connection.")


if __name__ == '__main__':
    main()

