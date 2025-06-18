import pandas as pd
import logging

logger = logging.getLogger(__name__)

class CoursesCleaner:
    '''
    Clean the `cademycode_courses` table according to the discoveries made in the writeup

    Parameters:
        df (DataFrame): `courses` table from `cademycode.db`

    Returns:
        df (DataFrame): cleaned version of the input table
    '''
    def __init__(self, df):
        self.df = df.copy()

    def clean(self):
        # Add a 'no career path' default row
        self.df.loc[len(self.df)] = [0, 'No career path', 0]
        return self.df
