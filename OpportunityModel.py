import pandas as pd
import numpy as np
import os
import re
from tqdm import tqdm
from typing import List
from pathlib import Path

def dat_reader(full_filepath: str) -> pd.DataFrame:
    basename = os.path.basename(full_filepath)
    filename_pattern = '^S(\d)-(ADL\d|Drill).*'
    file_re = re.match(pattern=filename_pattern, string=basename)
    df =  pd.read_csv(full_filepath, sep=' ', header=None)
    # Hack to search labels easier
    df.rename({243:'Locomotion',
           244:'HL_Activity',
           245:'LL_Left_Arm',
           246:'LL_Left_Arm_Object',
           247:'LL_Right_Arm',
           248:'LL_Right_Arm_Object',
           249:'ML_Both_Arms'}, axis='columns', inplace=True)
    df['file'] = basename
    df['PID'] = file_re.group(1)
    df['RunID'] = file_re.group(2)
    return df

class OppDF:
    """
        Class that handles the access and the filtering of the Opportunity dataset.
    """
    DATA_PICKLE_NAME = 'data.pkl'
    METADATA_PICKLE_NAME = 'metadata.pkl'
    LABELS_PICKLE_NAME = 'labels.pkl'

    def __init__(self):
        """
            Class Constructor. Ensures that the DataFrame variables are initialized.
        """
        self.df = None
        self.metadata = None
        self.labels = None

    def pickle_creation(self, data_folder: str, pickle_path: str):
        """
        Method that creates the pickles needed from Opportunity data
        Arguments:
            data_folder {str} -- The string of the path to the folder containing the Opportunity dat files.
            pickle_path {str} -- The string of the path to the folder that will contain the produced pickles.
        """
        label_file = Path(data_folder) / 'label_legend.txt'
        column_file = Path(data_folder) / 'column_names.txt'

        self.df = self.df_handler(data_folder)
        self.labels = self.label_handler(label_file.resolve())
        self.metadata = self.metadata_handler(column_file.resolve())

        data_pickle_path = Path(pickle_path) / self.DATA_PICKLE_NAME
        labels_pickle_path = Path(pickle_path) / self.LABELS_PICKLE_NAME
        metadata_pickle_path = Path(pickle_path) / self.METADATA_PICKLE_NAME

        self.labels.to_pickle(labels_pickle_path)
        self.df.to_pickle(data_pickle_path)
        self.metadata.to_pickle(metadata_pickle_path)

    def df_handler(self, data_folder: str) -> pd.DataFrame:
        """
        Method that creates a dataframe from the Opportunity dat files.
        Arguments:
            data_folder {str} -- Path to the folder containing the dat files
        Returns:
            pd.DataFrame -- DataFrame containing the Opportunity dataset.
        """
        data_files = Path(data_folder).glob('**/*.dat')
        df_list = []
        for file in tqdm(data_files):
            temp_df = dat_reader(file.resolve())
            df_list.append(temp_df)

        full_df = pd.concat(df_list, sort=False)
        return full_df

    def label_handler(self, label_file: str) -> pd.DataFrame:
        """
        Reads the label_file and creates a DataFrame containing that info.
        Arguments:
            label_file {str} -- Path to the labels txt file.
        Returns:
            pd.DataFrame -- DataFrame containing the labels info.
        """
        column_pattern = r"^(\d+)\s*-\s*([A-Za-z_]+)\s*-\s*([A-Za-z_ 1-9()]+)"
        label_list = []
        with open(label_file,'r') as f:
            for line in f:
                label_match = re.match(string = line.strip(), pattern = column_pattern)
                if label_match:
                    label_list.append(pd.DataFrame(data = [[label_match.group(1), label_match.group(2), label_match.group(3)]],
                                                   columns = ['Code', 'Class', 'Label']))
        return pd.concat(label_list)

    def metadata_handler(self, column_file: str) -> pd.DataFrame:
        """
        Reads the column_file and creates a DataFrame containing that info.
        Arguments:
            column_file {str} -- Path to the column_names txt file.
        Returns:
            pd.DataFrame -- DataFrame containing the labels info.
        """
        column_pattern = r"^Column: (\d+) ([A-Za-z]+) ([A-Za-z\d^_-]*) ([A-Za-z]*)"
        metadata = pd.DataFrame(data= [['Time','Time','Time']],columns = ['Sensor','Location','Signal'])
        with open(column_file,'r') as f:
            for line in f:
                ptrn_match = re.match(string=line.strip(), pattern = column_pattern)
                if ptrn_match:
                    temp_df = pd.DataFrame(data = [[ptrn_match.group(2), ptrn_match.group(3), ptrn_match.group(4)]],
                                        columns = ['Sensor','Location','Signal'])
                    metadata = metadata.append(temp_df, ignore_index=True)
        return metadata

    def populate(self, pickle_path: str):
        """
        Method that populates/updates the DataFrame using existing .
        Arguments:
            data_path {str} -- The string of the path to the folder containing the pickles.
        """
        data_pickle_path = Path(pickle_path) / self.DATA_PICKLE_NAME
        metadata_pickle_path = Path(pickle_path) / self.METADATA_PICKLE_NAME
        labels_pickle_path = Path(pickle_path) / self.LABELS_PICKLE_NAME

        self.df = pd.read_pickle(data_pickle_path)
        self.metadata = pd.read_pickle(metadata_pickle_path)
        self.labels = pd.read_pickle(labels_pickle_path)

