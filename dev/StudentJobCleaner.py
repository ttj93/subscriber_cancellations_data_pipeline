import pandas as pd
import logging

logger = logging.getLogger(__name__)

class StudentJobsCleaner:
    '''
    Clean the `cademycode_student_jobs` table according to the discoveries made in the writeup

    Parameters:
        df (DataFrame): `student_jobs` table from `cademycode.db`

    Returns:
        df (DataFrame): cleaned version of the input table
        maybe missing_data (DataFrame): incomplete data that was removed for later inspection
    '''
    def __init__(self, df):
        self.df = df.copy()

    def clean(self):
        self.df = self.df.drop_duplicates()
        return self.df


