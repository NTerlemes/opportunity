from OpportunityModel import OppDF
from OpportunityView import OppSelect
import Signal
import pickle
from pathlib import Path
from collections import defaultdict

class NestedDefaultDict(defaultdict):
    def __init__(self, *args, **kwargs):
        super(NestedDefaultDict, self).__init__(NestedDefaultDict, *args, **kwargs)

    def __repr__(self):
        return repr(dict(self))

def main():

    db = OppDF()
    db.populate("../results/pickles")
    db_view = OppSelect(db)
    unique_pids = list(db.df['PID'].unique())
    unique_locomotions = list(db.df['Locomotion'].unique())
    unique_run_ids = list(db.df['RunID'].unique())
    signal_info = NestedDefaultDict()

    for pid in unique_pids:
        for run_id in unique_run_ids:
            db_view.run_indexing({'PID': pid, 'RunID': run_id})
            for locomotion in unique_locomotions[1:]:
                db_view.label_indexing({'Locomotion': locomotion})
                time = db_view.df['Time']
                for signal in range(1,243):
                    data = db_view.df.iloc[:,signal]
                    temp_fs = Signal.FuzzySet(data)
                    signal_name = data.name
                    signal_info[pid][run_id][locomotion][signal_name]['fs'] = temp_fs
                    print("---"*10)
                    print(f"\tSignal:\t{signal_name}")
                    print(f"\tMin:\t{min(data)}")
                    print(f"\tMax:\t{max(data)}")
                db_view.restart()
            db_view.restart()
    result = dict(signal_info)
    print(result)
    print(type(result))
    with open('../results/fs/signal_info.pkl', 'wb') as f:
        pickle.dump(result, f)
    

if __name__ == "__main__":
    main()

