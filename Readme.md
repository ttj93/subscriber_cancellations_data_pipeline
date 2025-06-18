Read me

# Subscriber Cancellations Data Pipeline

## Project Description
- **Overview**:
  - A mock database of long-term cancelled subscribers for a fictional subscription company is regularly updated from multiple sources, and needs to be routinely cleaned and transformed into usable shape with as little human intervention as possible.
- **Purpose**:
  - To build a data engineering pipeline to regularly transform a messy database into a clean source of truth for an analytics team.
- **Goal**:
  A semi-automated pipeline:
    - Performs unit tests to confirm data validity
    - Writes human-readable errors to an error log
    - Automatically checks and updates changelogs
    - Updates a production database with new clean data

- ## Folder Structure

- **dev/**: Development directory
  - **changelog.md**: Changelog file to track updates to the database
  - **cademycode.db**: Original database containing raw data from 3 tables (`cademycode_students`, `cademycode_courses`, `cademycode_student_jobs`)
  - **cademycode_ds.db**: Cleansed database (created during the update process)
      - contains  table: `cademycode_aggregated`
  
- **prod/**: Production directory
  - **changelog.md**: `bashScript.sh` will copy from /dev when updates are approved
  - **cademycode_ds.db**: `bashScript.sh` will copy from /dev when updates are approved
  - **cademycode_ds.csv**: Aggregated data in CSV format for production use

- **writeup/**:
  - **WriteUp.ipynb**: Jupyter Notebook containing the discovery phase of this project: loading, inspecting, transforming.
    
- ##  Python Script


  - **script**: Controls the full ETL pipeline execution
    - Runs data validation unit tests
    - Executes data extraction, cleaning, merging, and upserting to the production DB
    - Exports the final dataset to a CSV
    - Logs process activity and auto-updates the changelog.md with version info and update summaries


  - **ETLPipeline**: Encapsulates the data pipeline logic
    - Loads raw data from the SQLite dev database
    - Applies cleaning steps using external cleaning modules
    - Merges cleaned data into a final dataframe
    - Updates production database (cademycode_ds.db) and exports to CSV


  - **StudentDataCleaner**: Cleans the cademycode_students table 
    - Parses and extracts fields from the contact_info JSON column
    - Converts data types (e.g., dob, job_id, etc.)
    - Fills or drops missing values
    - Adds a derived age column from dob
    - Contains run_all() for applying all transformations in order


  - **CoursesCleaner**: Cleans the cademycode_courses table  
    - Standardizes or replaces values like IDs for missing or invalid entries
    - Simplifies the table for merging with student data


  - **StudentJobCleaner**: Cleans the cademycode_student_jobs table  
    - Removes duplicate job entries
    - Standardizes job-related fields like job_category, avg_salary


  - **Test**: Unit Test Suite — Validates data at various ETL stages:  
    - TestNewData: Confirms there are new students to be added
    - TestStudentCleaner: Ensures proper extraction, type conversion, and missing value handling in student data
    - TestCoursesCleaner: Validates career path cleaner output (e.g., required placeholder values)
    - TestStudentJobCleaner: Checks duplicate removal and data consistency in job data
    - TestPreMerge: Ensures referential integrity before merging
    - TestSchema: Compares structure of final vs. production table

<details> <summary> Click to see ETL Pipeline Execution Flow</summary>


```
Script.py
├──  runs pre-ETL unit tests (tests/)
│   ├── Checks for new data
│   └── Ensures cleaning modules and dependencies load correctly
├──  runs ETLPipeline
│   ├──  loads raw tables (SQLite → Pandas DataFrames)
│   ├──  applies data cleaners:
│   │   ├── StudentDataCleaner: parses contact_info, fixes types, adds age
│   │   ├── StudentJobCleaner: de-duplicates, standardizes job data
│   │   └── CoursesCleaner: handles missing IDs, prepares for joins
│   └──  saves cleaned data
│       ├── to production SQLite DB (cademycode_ds.db)
│       └── to CSV file (cademycode_cleansed.csv)
└──  runs post-ETL unit tests 
    ├── Confirms cleaned tables meet schema expectations
    ├── Verifies no critical nulls or inconsistencies
    └── Tests DB and CSV output integrity
```

</details>

## Bash Script

- `bashScript.sh`: Bash script to handle running the Python script and copying updated files from the development directory to the production directory.

## Instructions

1. Make sure you have the required dependencies installed (Python and pandas).

2. Navigate to the project's root directory.

3. Run the `bashScript.sh` and follow the prompts to update the database.

4. If prompted, `bashScript.sh` will run `dev/clean_data.py`, which runs unit tests and data cleaning functions on `dev/cademycode.db`

5. If `script.py` runs into any errors during unit testing, it will raise an exception, log the issue, and terminate

6. Otherwise, `script.py` will update the clean database and CSV with any new records

7. After a successful update, the number of new records and other update data will be written to `dev/changelog.md`

8. `bashScript.sh` will check the changelog to see if there are updates

9. If so, `bashScript.sh` will request permission to overwrite the production database

10. If the user grants permission, `bashScript.sh` will copy the updated database to prod