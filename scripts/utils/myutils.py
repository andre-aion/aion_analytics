import pandas as pd
from os.path import join, dirname
from pandas.api.types import is_string_dtype
from datetime import datetime
import config

def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

def optimize_dataframe(df,timestamp_col='block_timestamp'):
    dtypes = df.drop(timestamp_col, axis=1).dtypes
    dtypes_col = dtypes.index
    dtypes_type = [i.name for i in dtypes.values]
    column_types = dict(zip(dtypes_col, dtypes_type))

    df_read_and_optimized = pd.read_csv(join(dirname(__file__), '../../data', 'blockdetails.csv'),
                                        dtype=column_types, parse_dates=['block_timestamp'],infer_datetime_format=True)

    return df_read_and_optimized


def convert_block_timestamp_from_string(df,col):
    if is_string_dtype(df[col]):
        df[col] = df[col].apply(int)
        df[col] = pd.Timestamp(df[col])
    return df

def setdatetimeindex(df):
    # set timestamp as index
    meta = ('block_timestamp', 'datetime64[ns]')
    df['block_timestamp'] = df['block_timestamp']
    df['block_timestamp'] = df.block_timestamp.map_partitions(pd.to_datetime, unit='s',
                                                              # format="%Y-%m-%d %H:%M:%S",
                                                              meta=meta)
    df = df.set_index('block_timestamp')
    return df


def get_month_from_timestamp(ts):
    time = datetime.fromtimestamp(ts)
    return time.month

