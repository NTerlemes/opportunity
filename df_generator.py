import os
import json
import re
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Tuple
from tqdm import tqdm
import shutil

data_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), '../Data/OpportunityUCIDataset/dataset')
results_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), '../results/pickles')

def rename_dict(df):

    column_file = Path('Data/OpportunityUCIDataset/dataset/column_names.txt').resolve()
    column_pattern = "^Column: (\d+) ([A-Za-z]+) ([A-Za-z\d^_-]*) ([A-Za-z]*)"
    with open(column_file, 'r') as f:
        for line in f:
            pattern_match = re.match(string=line, pattern=column_pattern)
            if pattern_match:
                print(line)

def metadata2columns(metadata: pd.DataFrame, what_we_want: dict) -> List[int]:
    """Given a description of the signal based on the 3 basic attributes
    Signal, Location and Sensor in the form of a dictonary with
    key the wanted attribute and value the wanted value of it,
    it returns the column of this signal.

    Arguments:
        metadata {pd.DataFrame} -- It retains the required information associating columns(signals) to their attributes.
        what_we_want {dict} -- Dictionary that specifies our query

    Returns:
        List[int] -- List of integers representing the columns(signals) that satisfy our query
    """
    attributes = list(what_we_want.keys())
    supported_attr = ['Signal', 'Location', 'Sensor']
    filtered_attr = [x for x in attributes if x in supported_attr]
    result = [True for i in range(metadata.shape[0])]
    for x in filtered_attr:
        temp_result = [False for i in range(metadata.shape[0])]
        for value in what_we_want[x]:
            t = metadata[x] == value
            temp_result = temp_result | t
        result = result & temp_result
    return np.where(result == True)[0].tolist()

def label_handler(data_folder: str) -> pd.DataFrame:
    column_pattern = "^(\d+)\s*-\s*([A-Za-z_]+)\s*-\s*([A-Za-z_ 1-9()]+)"
    label_list = []
    with open(os.path.join(data_folder, 'label_legend.txt'),'r') as f:
        for line in f:
            label_match = re.match(string = line.strip(), pattern = column_pattern)
            if label_match:
                label_list.append(pd.DataFrame(data = [[label_match.group(1), label_match.group(2), label_match.group(3)]],
                                               columns = ['Code', 'Class', 'Label']))
    return pd.concat(label_list)

def labels2index(labels, motion_class, motion_description):
    """
    Gets 3 inputs.
    A dataframe that includes the necessary data
    and the motion_class and description in order to find the index that describes it.
    Both motion_class and motion_description can be string or list of strings.
    The logical relationship applied between the class and description is AND.
    """
    if type(motion_class) is str:
        m_class = []
        m_class.append(motion_class)
    elif type(motion_class) is list:
        m_class = motion_class
    if type(motion_description) is str:
        m_description = []
        m_description.append(motion_description)
    elif type(motion_description) is list:
        m_description = motion_description

    result = labels[labels['Class'].isin(m_class) & (labels['Label'].isin(m_description))]
    return result.to_dict('records')

def column_name_handler(data_folder: str) -> pd.DataFrame:

    column_pattern = "^Column: (\d+) ([A-Za-z]+) ([A-Za-z\\d^_-]*) ([A-Za-z]*)"
    metadata = pd.DataFrame(data= [['Time','Time', 'Time']],columns = ['Sensor','Location','Signal'])
    with open(os.path.join(data_folder, 'column_names.txt'),'r') as f:
        for line in f:
            ptrn_match = re.match(string=line.strip(), pattern = column_pattern)
            if ptrn_match:
                temp_df = pd.DataFrame(data = [[ptrn_match.group(2), ptrn_match.group(3), ptrn_match.group(4)]],
                                       columns = ['Sensor','Location','Signal'])
                metadata = metadata.append(temp_df, ignore_index=True)
    return metadata

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

def df_generator() -> Tuple[pd.DataFrame, pd.DataFrame]:
    data_files = list(filter(lambda x: x.endswith('.dat'), os.listdir(data_folder)))

    df_list = []
    metadata = column_name_handler(data_folder)
    # Filename pattern for retrieving run_metadata
    # Metadata:
    # PID : Participant ID
    # RunID : Which run and iteration this file represents
    for file in tqdm(data_files):

        temp_df = dat_reader(os.path.abspath(os.path.join(data_folder,file)))
        df_list.append(temp_df)

    full_df = pd.concat(df_list, sort=False)
    return (full_df, metadata)

if __name__ == "__main__":

    data_pickle_path = os.path.join(results_folder, 'data.pkl')
    metadata_pickle_path = os.path.join(results_folder, 'metadata.pkl')
    labels_pickle_path = os.path.join(results_folder, 'labels.pkl')

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)
    if not os.path.isfile(data_pickle_path) or not os.path.isfile(metadata_pickle_path):
        shutil.rmtree(results_folder)
        os.makedirs(results_folder)
        df, metadata = df_generator()
        labels = label_handler(data_folder)
        labels.to_pickle(labels_pickle_path)
        df.to_pickle(data_pickle_path)
        metadata.to_pickle(metadata_pickle_path)
    else:
        print('Already existing pickles')
        df = pd.read_pickle(data_pickle_path)
        metadata = pd.read_pickle(metadata_pickle_path)
        labels = pd.read_pickle(labels_pickle_path)

