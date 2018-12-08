from scripts.utils.mylogger import mylogger
import pandas as pd
from os.path import join, dirname
from pandas.api.types import is_string_dtype
from datetime import datetime, date
import dask as dd
from bokeh.models import Panel
from bokeh.models.widgets import Div
import numpy as np
from tornado.gen import coroutine
import config

logger = mylogger(__file__)

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


def get_breakdown_from_timestamp(ts):
    ns = 1e-6
    mydate = datetime.fromtimestamp(ts).date()
    return mydate.month, mydate

def get_initial_blocks(pc):
    try:
        to_check = tuple(range(0, 15000))
        qry ="""SELECT block_number, difficulty, block_date, 
            block_time, miner_addr FROM block WHERE
            block_number in """+str(to_check)

        df = pd.DataFrame(list(pc.session.execute(qry)))
        df = dd.dataframe.from_pandas(df, npartitions=15)
        #logger.warning('%s',df.head(5))
        return df
    except Exception:
        logger.error('get initial blocks',exc_info=True)


def timestamp_to_datetime(ts):
    return datetime.fromtimestamp(ts)


# when a tab does not work
def tab_error_flag(tabname):
    print('IN POOLMINER')

    # Make a tab with the layout
    div = Div(text="""ERROR CREATING POOLMINER TAB, 
    CHECK THE LOGS""",
              width=200, height=100)

    tab = Panel(child=div, title=tabname)

    return tab


# convert dates from timestamp[ms] to datetime[ns]
def ms_to_date(ts):
    try:
        if isinstance(ts, int) == True:
            # change milli to seconds
            if ts > 163076320:
                ts = ts // 1000
            ts = datetime.utcfromtimestamp(ts)
            # convert to nanosecond representation
            ts = np.datetime64(ts).astype(datetime)
            ts = pd.Timestamp(datetime.date(ts))

            logger.warning('from ms_to_date: %s',ts)
        return ts
    except Exception:
        logger.error('ms_to_date', exc_info=True)
        return ts


# nano_secs_to_date
def ns_to_date(ts):
    ns = 1e-9
    try:
        ts = datetime.utcfromtimestamp(ts * ns)
        ts = pd.Timestamp(datetime.date(ts))
        return ts
    except Exception:
        logger.error('ns_to_date', exc_info=True)
        return ts
