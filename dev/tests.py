import unittest
import pandas as pd
import sqlite3
from StudentDataCleaner import StudentCleaner
from CoursesCleaner import CoursesCleaner
from StudentJobCleaner import StudentJobsCleaner
import json
import logging
import os


logger = logging.getLogger(__name__)
class TestNewData(unittest.TestCase):
    #checking if there is any new data
    def setUp(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.prod_path = os.path.join(self.base_dir, 'cademycode_ds.db')
        self.dev_path = os.path.join(self.base_dir, 'cademycode.db')
        self.dev_con = sqlite3.connect(self.dev_path)
        self.prod_con = sqlite3.connect(self.prod_path)

        # Step 1: Load both  tables
        self.dfStudent = pd.read_sql_query("SELECT * FROM cademycode_students", self.dev_con)
        self.prod_db = pd.read_sql("SELECT * FROM cademycode_aggregated", self.prod_con)

        # Step 2: Get unique IDs from production
        self.existing_ids = set(self.prod_db['uuid'].unique())

        # Step 3: Filter only new records (ID not in existing_ids)
        # new_students is the dataframe with only new students not in prod. this df is the input for the classes methods
        self.new_students = self.dfStudent[~self.dfStudent['uuid'].isin(self.existing_ids)]


    def test_new_data(self):
        #if len new_student > 0 that means there is new data that is not in the prod table yet
        self.assertTrue((len(self.new_students) > 0 ),"Error test_new_data no new records found")


class TestStudentCleaner(unittest.TestCase):
    def setUp(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.prod_path = os.path.join(self.base_dir, 'cademycode_ds.db')
        self.dev_path = os.path.join(self.base_dir, 'cademycode.db')
        self.dev_con = sqlite3.connect(self.dev_path)
        self.prod_con = sqlite3.connect(self.prod_path)
        self.df_dev= pd.read_sql("SELECT * FROM cademycode_students", self.dev_con)
        self.df_prod = pd.read_sql("SELECT * FROM cademycode_aggregated", self.prod_con)
        self.contact = json.dumps({"mailing_address": "303 N Timber Key, Irondale, Wisconsin, 84736","email":"annabelle_avery9376@woohoo.com"})
        self.data = {
            "uuid": [888,889],
            "name": ["John Doe", "Jane Smith"],
            "dob": ["2002-08-22","1998-05-10"],
            "sex": ["M","F"],
            "contact_info": [self.contact, self.contact],
            "job_id": [None, 3],
            "num_course_taken": [None, 5],
            "current_career_path_id": [None, None],
            "time_spent_hrs": [None, None]
        }
        self.type = ['job_id','num_course_taken','current_career_path_id','time_spent_hrs']
        self.df = pd.DataFrame(self.data)
        self.cleaner = StudentCleaner(self.df)

    def test_dict_contact_info(self):
        split = self.cleaner.dict_contact_info(self.contact)
        self.assertEqual(split['mailing_address'], "303 N Timber Key, Irondale, Wisconsin, 84736"), 'Error dict_contact_info mailing address is wrong'
        self.assertEqual(split['email'], "annabelle_avery9376@woohoo.com"), 'Error dict_contact_info email is wrong'
        #self.assertEqual(True, False)  # add assertion here

    def test_clean(self):
        cleaned_df = self.cleaner.clean()
        self.assertIn('mailing_address', cleaned_df.columns), 'Error test clean: mailing_address not found in columns'
        self.assertIn('email', cleaned_df.columns), 'Error test clean: email not found in columns'
        self.assertNotIn('contact_info', cleaned_df.columns), 'Error test clean: contact_info  found in columns'

    def test_change_dtype(self):
        changed_dtype = self.cleaner.change_dtype()
        for column in self.type:
            with self.subTest(column):
                message = 'Error test_change_dtype: (' + str(column) + ') has incorrect type'
                self.assertEqual(changed_dtype[column].dtype, 'float64', message)
        self.assertEqual(changed_dtype['dob'].dtype, '<M8[ns]', 'Error test_change_dtype: dob has as incorrect type')

    def test_missing_data(self):
        missing_df = self.cleaner.missing_data()
        self.assertFalse(missing_df['job_id'].isnull().any()), 'Error test_missing_data there are null values for job_id'
        self.assertFalse(missing_df['num_course_taken'].isnull().any()), 'Error test_missing_data there are null values for num_course_taken'
        self.assertTrue((missing_df['current_career_path_id'] == 0).any(), "Error test_missing_data 0 not found in current_career_path_id")
        self.assertTrue((missing_df['time_spent_hrs'] == 0).any(), "Error test_missing_data 0 not found in time_spent_hrs")

    def test_age_column(self):
        self.cleaner.change_dtype()  # convert dob to datetime
        age_column_df = self.cleaner.age_column()
        self.assertIn('age', age_column_df), 'Error test_age_column: age not found in columns'


class TestCoursesCleaner(unittest.TestCase):
    def setUp(self):
        self.data = {
            "career_path_id": [1, 2],
            "career_path_name": ["data scientist", "data engineer"],
            "hours_to_complete": [20, 20]
        }
        self.df = pd.DataFrame(self.data)
        self.cleaner = CoursesCleaner(self.df)

    def test_clean(self):
        cleaned_df = self.cleaner.clean()
        self.assertTrue((cleaned_df['career_path_id'] == 0).any(), "Error test_clean 0 not found in career_path_id")


class TestStudentJobCleaner(unittest.TestCase):
    def setUp(self):
        self.data = {
            "job_id": [1, 3, 3],
            "job_category": ["analytics", "software developer", "software developer"],
            "avg_salary": [20, 110000, 110000]
        }
        self.df = pd.DataFrame(self.data)
        self.cleaner = StudentJobsCleaner(self.df)

    def test_clean(self):
        cleaned_df = self.cleaner.clean()
        self.assertEqual(cleaned_df.duplicated().sum(), 0), "Error test_clean duplicates found in StudentJob"


class TestPreMerge(unittest.TestCase):
    """
    Unit test to ensure that join keys exist between the students and courses tables

    Parameters:
        cleaned students (DataFrame): `cademycode_student_jobs` table from `cademycode.db`
        cleaned career_paths (DataFrame): `cademycode_courses` table from `cademycode.db`
        cleaned student_jobs (DataFrame): `cademycode_courses` table from `cademycode.db`

    Returns:
        None
    """
    def setUp(self):
        self.cleaned_students = TestPreMerge.cleaned_students
        self.cleaned_careers = TestPreMerge.cleaned_careers
        self.cleaned_jobs = TestPreMerge.cleaned_jobs

    def test_match(self):
        students =  self.cleaned_students.current_career_path_id
        studentsJob = self.cleaned_students.job_id

        missing_keys_path = students[~students.isin(self.cleaned_careers.career_path_id)]
        missing_keys_job = studentsJob[~studentsJob.isin(self.cleaned_jobs.job_id)]

        self.assertEqual(len(missing_keys_path.index), 0, f"Missing career path IDs: {missing_keys_path.tolist()}")
        self.assertEqual(len(missing_keys_job.index), 0, f"Missing job IDs: {missing_keys_job.tolist()}")


class TestSchema(unittest.TestCase):
    def setUp(self):
        self.final_df = TestSchema.final_df
        self.prod_df = TestSchema.prod_df

    def test_check_schema(self):
        self.assertEqual(len(self.final_df.columns), len(self.prod_df.columns), "Error test_schema 0 not found in final_df")

        #sqlite 3 doesnt support datatype datetime so this functionality doesn't work, it is checking datatype
        """errors = 0
        for col in self.prod_df:
            if self.final_df[col].dtypes != self.prod_df[col].dtypes:
                print(self.prod_df['dob'])
                errors += 1

        if errors > 0:
            logger.exception(str(errors) + " column(s) dtypes aren't the same")
            assert errors == 0, str(errors) + " column(s) dtypes aren't the same"
        """


if __name__ == '__main__':
    unittest.main()
