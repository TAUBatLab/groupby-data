import numpy as np
import pandas as pd
import scipy.io as sio
from datetime import datetime, timedelta

def mat_to_py_time(matlab_datenum):
    python_datetime = datetime.fromordinal(int(matlab_datenum)) + timedelta(days=matlab_datenum%1) - timedelta(days = 366)
    return python_datetime

def abs_value(value):
    value = abs(value)
    return value

class GroupData:

    def __init__(self, fname, output1, output2):
        self.fname = fname
        self.output1 = output1
        self.output2 = output2
        self.df = None

    def mat_to_df(self):
        mat = sio.loadmat(self.fname)
        time_acc = mat['timeAcc'][0:2000] # specific data
        new_z = mat['new_z'][0:2000] # specific data
        # time_acc = mat['timeAcc'] # all data
        # new_z = mat['new_z'] # all data
        df_time = pd.DataFrame(data = time_acc, columns=['Time'])
        df_acc = pd.DataFrame(data = new_z, columns=['Acc'])
        df_all = pd.concat([df_time, df_acc], axis=1, sort = True)
        df = df_all.copy()
        df['real time'] = df['Time'].apply(mat_to_py_time)
        df['Acc'] = df['Acc'].apply(abs_value)
        df.pop('Time')
        self.df = df
        # return df
        # print (df2)

    def groupby_df(self):

        self.df['real time'] = pd.to_datetime(self.df['real time'])
        hour = pd.to_timedelta(self.df['real time'].dt.hour, unit='H')
        day = pd.to_timedelta(self.df['real time'].dt.day, unit='d')

        # mean_h = df.groupby(hour).mean()
        # mean_d = df.groupby(day).mean()
        # day_hour_mean = df.groupby([day, hour]).mean()
        # day_hour_mean = df.groupby([day, hour]).mean()
        # day_hour_min = df.groupby([day, hour]).min()
        # day_hour_max = df.groupby([day, hour]).max()
        # day_hour_max = df.groupby([day, hour]).std()
        # day_hour_count = df.groupby([day, hour]).count()

        grouped_day = self.df.groupby([day]).aggregate(['min', max, np.median, np.std, np.mean])
        grouped_day.to_csv(self.output1)
        grouped_day_hour = self.df.groupby([day, hour]).aggregate(['min', max, np.median, np.std, np.mean])
        grouped_day_hour.to_csv(self.output2)
        

        # grouped = pd.DataFrame(columns = ['count'],['mean'],['max'],['min'],['std'])

        # print (day_hour_mean, day_hour_min, day_hour_max)
        # print (grouped)
        # return grouped_day, grouped_day_hour

    def run (self):
        self.mat_to_df()
        self.groupby_df()





if __name__ == "__main__":

    fname = 'SmoothedFile.mat' # write here file name
    output1 = "grouped_day.csv" # change output file name here
    output2 = "grouped_data_day_hour.csv" # change output file name here

    yeals_data = GroupData (fname, output1, output2)
    yeals_data.run()
  


  
  