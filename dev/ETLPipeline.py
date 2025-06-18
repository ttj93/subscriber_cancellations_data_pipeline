import sqlite3
import pandas as pd
from StudentDataCleaner import StudentCleaner
from CoursesCleaner import CoursesCleaner
from StudentJobCleaner import StudentJobsCleaner
import logging
import os


logger = logging.getLogger(__name__)
class ETLPipeline:
    '''
    Clean the `cademycode_students, courses, student_jobs` tables according to the discoveries made in the writeup

    Parameters:
        db (cademycode.db): containing the different tables

    Returns:
        -df (DataFrames): cleaned versions of the students, careers, jobs  tables
        -merged table
        -csv of the merged df / table

    '''
    def __init__(self, db_path):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.dev_path = os.path.join(self.base_dir, db_path)
        self.dev_con = sqlite3.connect(self.dev_path)
        self.cleaned_students = None
        self.cleaned_careers = None
        self.cleaned_jobs = None

    def load_data(self):
        students = pd.read_sql_query("SELECT * FROM cademycode_students", self.dev_con)
        careers = pd.read_sql_query("SELECT * FROM cademycode_courses", self.dev_con)
        jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", self.dev_con)
        self.dev_con.close()
        return students, careers, jobs

    def clean_data(self, students_df, careers_df, jobs_df):
        self.cleaned_students = StudentCleaner(students_df).run_all()
        self.cleaned_careers = CoursesCleaner(careers_df).clean()
        self.cleaned_jobs = StudentJobsCleaner(jobs_df).clean()
        return self.cleaned_students, self.cleaned_careers, self.cleaned_jobs

    def merge_cleaned_tables(self, cleaned_students, cleaned_careers, cleaned_jobs):
        self.final_df = cleaned_students.merge(cleaned_careers, left_on='current_career_path_id', right_on='career_path_id', how='left')
        self.final_df = self.final_df.merge(cleaned_jobs, on='job_id', how='left')
        return self.final_df

    def upsert_to_prod(self, final_df, prod_con):
        final_df.to_sql('cademycode_aggregated', prod_con, if_exists='replace', index=False)



    def export_csv(self,prod_con):
        db_df = pd.read_sql_query("SELECT * FROM cademycode_aggregated", prod_con)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_dir, "cademycode_ds.csv")
        db_df.to_csv(csv_path, index=False)
        return csv_path
        #return db_df.to_csv('cademycode_ds.csv')