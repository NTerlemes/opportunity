from OpportunityModel import OppDF
from typing import List


class OppSelect:
    def __init__(self, base_model: OppDF):
        self.base_df = base_model.df
        self.last_df = base_model.df
        self.df = base_model.df
        self.labels = base_model.labels
        self.metadata = base_model.metadata

        self.columns = self.metadata.to_dict("index")

    def signal_indexing(self, filter_dict: dict):
        """
        Given a dataset and metadata on it, this function retains
        only the signals mentioned in filter_dict dictionary.
        Arguments:
            filter_dict {dict} -- Dictionary that specifies our query.
            E.g. {'Signal': ['accX', 'accY', 'accZ'], 'Location': 'BACK'}
        Return:
            pd.Dataframe -- Filtered df
        """
        labels_columns = list(self.labels['Class'].unique())
        metadata_columns = ["file", "PID", "RunID"]
        time_column = ["Time"]
        result_columns = self.metadata2columns(filter_dict)
        selected_columns_index = list(result_columns.keys())
        print(selected_columns_index)
        selected_columns_names = list(self.df.columns[selected_columns_index])
        print(selected_columns_names)
        self.last_df = self.df
        selected_columns = (time_column + selected_columns_names +
                            labels_columns + metadata_columns)
        self.df = self.df[selected_columns]
        self.columns = result_columns

    def metadata2columns(self, filter_dict: dict) -> List[int]:
        """Given a description of the signal based on the 3 basic attributes
        Signal, Location and Sensor in the form of a dictonary with
        key the wanted attribute and value the wanted value of it,
        it returns the column of this signal.
        E.g. metadata2columns(
        {'Signal': ['accX', 'accY', 'accZ'],
         'Location': 'BACK'})

        Arguments:
            filter_dict {dict} -- Dictionary that specifies our query

        Returns:
            List[int] -- List of integers representing the columns of
            (signals) that satisfy our query
        """
        attributes = list(filter_dict.keys())
        supported_attr = ["Signal", "Location", "Sensor"]
        filtered_attr = [x for x in attributes if x in supported_attr]
        temp = self.metadata
        for x in filtered_attr:
            if type(filter_dict[x]) is str:
                filter_dict[x] = [filter_dict[x]]
            temp = temp[temp[x].isin(filter_dict[x])]
        return temp.to_dict("index")

    def label_indexing(self, filter_dict: dict):
        """
        A function that filters a dataframe based
        on motion_class and motion_decription.
        E.g. : temp = self.label_indexing('Locomotion', ['Sit', 'Walk'])
        Input:
        motion_class : List or string of the motion_class/type
        motion_description : List or String of the motion_description.
        Output:
        filtered_df : A panda dataframe with data
        that describe only the required descriptions.
        """
        self.last_df = self.df
        produced_labels = self.labels2index(filter_dict)
        temp = [False] * self.df['Time'].count()
        motion_class = list(map(lambda x: x["Class"], produced_labels))
        for m in motion_class:
            regarding_m = list(filter(lambda x: x["Class"] == m,
                                      produced_labels))
            index_only = list(map(lambda x: x["Label"], regarding_m))
            temp_result = self.df[m].isin(index_only)
            temp = temp | temp_result
        self.df = self.df[temp]

    def labels2index(self, filter_dict):
        """
        Gets 3 inputs.
        A dataframe that includes the necessary data
        and the motion_class and description in order
        to find the index that describes it.
        Both motion_class and motion_description can be string
        or list of strings.
        The logical relationship applied between the class
        and description is AND.
        """
        labels = self.labels
        result = []
        for key in filter_dict:
            if type(filter_dict[key]) is str:
                filter_dict[key] = [filter_dict[key]]
            logical_indexing = (labels["Class"] == key) & (
                labels["Label"].isin(filter_dict[key])
            )
            result.extend(labels[logical_indexing].to_dict("records"))
        return result

    def run_indexing(self, filter_dict):
        """
        Method that filters the DataFrame based on PID and RunID
        Arguments:
            filter_dict: dict Dictionary has fields PID and RunID
        """
        pids = filter_dict.get("PID", [])
        if type(pids) is str:
            pids = [pids]
        runs = filter_dict.get("RunID", [])
        if type(runs) is str:
            runs = [runs]
        if pids or runs:
            self.last_df = self.df
        for pid in pids:
            self.df = self.df[self.df["PID"] == pid]
        for run in runs:
            self.df = self.df[self.df["RunID"] == run]

    def restart(self):
        self.df = self.base_df

    def undo(self):
        self.df = self.last_df
