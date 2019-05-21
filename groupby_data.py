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

def mat_to_df(fname):
    mat = sio.loadmat(fname)
    # time_acc = mat['timeAcc'][1000:100000] # specific data
    # new_z = mat['new_z'][1000:100000] # specific data
    time_acc = mat['timeAcc'] # all data
    new_z = mat['new_z'] # all data
    df_time = pd.DataFrame(data = time_acc, columns=['Time'])
    df_acc = pd.DataFrame(data = new_z, columns=['Acc'])
    df_all = pd.concat([df_time, df_acc], axis=1, sort = True)
    df = df_all.copy()
    df['real time'] = df['Time'].apply(mat_to_py_time)
    df['Acc'] = df['Acc'].apply(abs_value)
    df.pop('Time')
    return df
    # print (df2)

def groupby_df(df):

    df['real time'] = pd.to_datetime(df['real time'])
    hour = pd.to_timedelta(df['real time'].dt.hour, unit='H')
    day = pd.to_timedelta(df['real time'].dt.day, unit='d')

    # mean_h = df.groupby(hour).mean()
    # mean_d = df.groupby(day).mean()
    # day_hour_mean = df.groupby([day, hour]).mean()
    # day_hour_mean = df.groupby([day, hour]).mean()
    # day_hour_min = df.groupby([day, hour]).min()
    # day_hour_max = df.groupby([day, hour]).max()
    # day_hour_max = df.groupby([day, hour]).std()
    # day_hour_count = df.groupby([day, hour]).count()

    grouped = df.groupby([day, hour]).aggregate(['min', max, np.median, np.std, np.mean])
    grouped.to_csv("grouped_data.csv")
    

    # grouped = pd.DataFrame(columns = ['count'],['mean'],['max'],['min'],['std'])

    # print (day_hour_mean, day_hour_min, day_hour_max)
    # print (grouped)
    return grouped





if __name__ == "__main__":

    fname = 'SmoothedFile.mat'
    data = mat_to_df(fname)
    # print (data)
    data_grouped = groupby_df(data)

    # pd.DataFrame(columns=['two', 'three'])

  
  