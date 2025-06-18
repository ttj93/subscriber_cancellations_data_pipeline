import pandas as pd
import sqlite3
import json
import logging

logger = logging.getLogger(__name__)
class StudentCleaner:
    '''
    Clean the `cademycode_students` table according to the discoveries made in the writeup

    Parameters:
        df (DataFrame): `student` table from `cademycode.db`

    Returns:
        df (DataFrame): cleaned version of the input table
    '''
    def __init__(self, df):
        self.df = df.copy()

    def dict_contact_info(self, contact_info):
        dict = json.loads(contact_info)
        return pd.Series(dict)

    def clean(self):
        # Split and expand the contact_info JSON
        split_contact = self.df['contact_info'].apply(self.dict_contact_info)
        self.df['mailing_address'] = split_contact['mailing_address']
        self.df['email'] = split_contact['email']
        self.df = self.df.drop('contact_info', axis=1)
        return self.df

    def change_dtype(self):
    # Changing datatype
        self.df = self.df.astype({
            'job_id': 'float',
            'num_course_taken': 'float',
            'current_career_path_id': 'float',
            'time_spent_hrs': 'float'
        })
        self.df['dob'] = pd.to_datetime(self.df['dob'])
        return self.df

    def missing_data(self):
        # Handling missing data
        self.df = self.df.dropna(subset=["job_id"])
        self.df = self.df.dropna(subset=["num_course_taken"])
        self.df['current_career_path_id'] = self.df['current_career_path_id'].fillna(value=0)
        self.df['time_spent_hrs'] = self.df['time_spent_hrs'].fillna(value=0)
        return self.df

    def age_column(self):
        # Adding age column from dob
        # Calculate age by subtracting birth year from current year, then adjust by subtracting 1 if the birthday hasn’t happened yet this year.
        now = pd.to_datetime('now')
        self.df['age'] = now.year - self.df['dob'].dt.year - (
                (now.month < self.df['dob'].dt.month) |
                ((now.month == self.df['dob'].dt.month) & (now.day < self.df['dob'].dt.day)))
        return self.df

    def run_all(self):
        self.clean()
        self.change_dtype()
        self.missing_data()
        self.age_column()
        return self.df

