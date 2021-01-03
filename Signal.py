import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import json
import math
from sklearn.preprocessing import scale

class Signal:
    def __init__(self, time, data):
    
        temp = pd.DataFrame({"Time": time, "Signal": data})
        temp = temp.dropna()
        self.time = temp['Time']
        self.data = temp['Signal']
        self.range = (max(self.data)-min(self.data))
        self.norm_data = scale(self.data,
                                     axis=0,
                                     with_mean=True,
                                     with_std=True,
                                     copy=True)
        self.gradient = np.diff(self.data)
        self.norm_gradient = np.diff(self.norm_data)
        
        self.fuzzy_set = FuzzySet(self.data)
        self.norm_fuzzy_set = FuzzySet(self.norm_data)
        self.grad_fuzzy_set = FuzzySet(self.gradient)
        self.norm_grad_fuzzy_set = FuzzySet(self.norm_gradient)
        
    def plot(self, other=None, normalized=False, gradient=False):
        
        if normalized and gradient:
            plot_data = self.norm_gradient
            plot_time = self.time[1:]
            if other:
                other_data = other.norm_gradient
                other_time = other.time[1:]
        elif normalized and not gradient:
            plot_data = self.norm_data
            plot_time = self.time
            if other:
                other_data = other.norm_data
                other_time = other.time
        elif not normalized and gradient:
            plot_data = self.gradient
            plot_time = self.time[1:]
            if other:
                other_data = other.gradient
                other_time = other.time[1:]
        elif not normalized and not gradient:
            plot_data = self.data
            plot_time = self.time
            if other:
                other_data = other.data
                other_time = other.time
        else:
            raise Exception('boom')

        if not other:
            plt.plot(plot_time, plot_data)
        else:
            plt.plot(plot_time, plot_data,
                     other_time, other_data)

    def fs_plot(self, other=None, normalized=False, gradient=False):
        
        if normalized and gradient:
            target_fs = self.norm_grad_fuzzy_set
            if other:
                other_fs = other.norm_grad_fuzzy_set
        elif normalized and not gradient:
            target_fs = self.norm_fuzzy_set
            if other:
                other_fs = other.norm_fuzzy_set
        elif not normalized and gradient:
            target_fs = self.grad_fuzzy_set
            if other:
                other_fs = other.grad_fuzzy_set
        elif not normalized and not gradient:
            target_fs = self.fuzzy_set
            if other:
                other_fs = other.fuzzy_set
        else:
            raise Exception('boom')

        if not other:
            target_fs.plot()
        else:
            target_fs.plot(other_fs)

    def compare(self, other):
        ...

class FuzzySet:
    def __init__(self, signal, mode='density'):
        
        signal_range = (max(signal)-min(signal))
        num_of_bins = np.max([math.ceil(math.sqrt(signal_range) / 10) * 10,
                              50])
        self.hist, self.bin_edges = np.histogram(
            signal, bins=num_of_bins, density=True
        )
    
    def compare(self, other):
        if not (type(other) == self.__class__):
            raise Exception

    def plot(self, other=None):
        if not other:
            temp_hist = np.insert(self.hist, 0, self.hist[0])
            plt.plot(self.bin_edges, temp_hist,
                     '-', drawstyle='steps')
        else:
            temp_hist_1 = np.insert(self.hist, 0, self.hist[0])
            temp_hist_2 = np.insert(other.hist, 0, other.hist[0])
            plt.plot(self.bin_edges, temp_hist_1,
                     other.bin_edges, temp_hist_2,
                     '-', drawstyle='steps')