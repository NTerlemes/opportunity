from pathlib import Path
from source.OpportunityModel import OppDF
import pandas as pd
from copy import deepcopy
from typing import List

class OppSelect():

    def __init__(self, base_model: OppDF):
        self.df = deepcopy(base_model.df)
        self.labels = deepcopy(base_model.labels)
        self.metadata = deepcopy(base_model.metadata)

        self.columns = self.metadata.to_dict('index')

    def signal_filtering(self, filter_dict: dict):
        """
        Given a dataset and metadata on it, this function retains only the signals mentioned in filter_dict dictionary.
        Arguments:
            filter_dict {dict} -- Dictionary that specifies our query. E.g. {'Signal': ['accX', 'accY', 'accZ'], 'Location': 'BACK'}
        Return:
            pd.Dataframe -- Filtered df
        """
        labels_columns = ['Locomotion', 'HL_Activity', 'LL_Left_Arm', 'LL_Left_Arm_Object', 'LL_Right_Arm', 'LL_Right_Arm_Object', 'ML_Both_Arms']
        metadata_columns = ["file", "PID", "RunID"]
        time_column = [0]
        result_columns = self.metadata2columns(filter_dict)
        columns = list(result_columns.keys())
        self.df = self.df[time_column + columns + labels_columns + metadata_columns]
        self.columns = result_columns

    def metadata2columns(self, filter_dict: dict) -> List[int]:
        """Given a description of the signal based on the 3 basic attributes
        Signal, Location and Sensor in the form of a dictonary with
        key the wanted attribute and value the wanted value of it,
        it returns the column of this signal.
        E.g. metadata2columns({'Signal': ['accX', 'accY', 'accZ'], 'Location': 'BACK'})

        Arguments:
            filter_dict {dict} -- Dictionary that specifies our query

        Returns:
            List[int] -- List of integers representing the columns(signals) that satisfy our query
        """
        attributes = list(filter_dict.keys())
        supported_attr = ['Signal', 'Location', 'Sensor']
        filtered_attr = [x for x in attributes if x in supported_attr]
        temp = self.metadata
        for x in filtered_attr:
            if type(filter_dict[x]) is str:
                filter_dict[x] = [filter_dict[x]]
            temp = temp[temp[x].isin(filter_dict[x])]
        return temp.to_dict('index')
