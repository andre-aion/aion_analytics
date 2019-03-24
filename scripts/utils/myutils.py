from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from operator import xor

from numpy import inf

from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.databases.pythonParquet import PythonParquet

import pandas as pd
from os.path import join, dirname
from pandas.api.types import is_string_dtype
from datetime import datetime, timedelta
import dask as dd
from bokeh.models import Panel
from bokeh.models.widgets import Div
import numpy as np
import gc

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=5)

class LoadType(Enum):
    DISK_STORAGE_FULL = 2
    DISK_STORAGE_START = 4
    DISK_STORAGE_END = 8
    DISK_STORAGE_LOADED = 16


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

    df_read_and_optimized = pd.read_csv(join(dirname(__file__),
                                             '../../data', 'blockdetails.csv'),
                                             dtype=column_types, parse_dates=['block_timestamp'],
                                             infer_datetime_format=True)

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
    df['block_timestamp'] = df.block_timestamp\
        .map_partitions(pd.to_datetime, unit='s',
                                        format="%Y-%m-%d %H:%M:%S",
                                        meta=meta)
    df = df.set_index('block_timestamp')
    return df


def get_breakdown_from_timestamp(ts):
    ns = 1e-6
    mydate = datetime.fromtimestamp(ts).date()
    return mydate


def timestamp_to_datetime(ts):
    return datetime.fromtimestamp(ts)


def make_filepath(path):
    return join(dirname(__file__), path)

# when a tab does not work
def tab_error_flag(tabname):

    # Make a tab with the layout
    text = """ERROR CREATING {} TAB, 
    CHECK THE LOGS""".format(tabname.upper())
    div = Div(text=text,
              width=200, height=100)

    tab = Panel(child=div, title=tabname)

    return tab
# when a tab does not work
def tab_disabled_flag(tabname):

    # Make a tab with the layout
    text = """To launch the {} tab, go back to the selection tab, </br>
    and enable it by selecting the <i>{}</i> checkbox.""".format(tabname.upper(),tabname)
    div = Div(text=text,
              width=600, height=100)

    tab = Panel(child=div, title=tabname)

    return tab

# convert dates from timestamp[ms] to datetime[ns]
def ms_to_date(ts):
    try:
        if isinstance(ts, int):
            # change milli to seconds
            if ts > 16307632000:
                ts = ts // 1000
            ts = datetime.utcfromtimestamp(ts)
            # convert to nanosecond representation
            ts = np.datetime64(ts).astype(datetime)
            ts = pd.Timestamp(datetime.date(ts))

            logger.warning('from ms_to_date: %s',ts)
        elif isinstance(ts,str):
            ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
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

# date time to ms
def date_to_ms(ts):
    if isinstance(ts, str):
        ts = datetime.strptime(ts, '%Y-%m-%d')

    ts = int(ts.timestamp())
    return ts



#convert ms to string date
def slider_ts_to_str(ts):
    # convert to datetime if necessary
    if isinstance(ts,int) == True:
        ts = ms_to_date(ts)
    if isinstance(ts,datetime):
        ts = datetime.strftime(ts,'%Y-%m-%d')
    return ts


def drop_cols(df,cols_to_drop):
    to_drop = []
    for col in cols_to_drop:
        if col in df.columns.tolist():
            to_drop.append(col)
    if len(to_drop) > 0:
        df = df.drop(to_drop, axis=1)
    return df

def clean_data(df):
    df = df.fillna(0)
    df[df == -inf] = 0
    df[df == inf] = 0
    return df

#
# check to see if the current data is within the active dataset
def set_params_to_load(df, req_start_date, req_end_date):
    try:
        if isinstance(req_start_date, int):
            req_start_date = ms_to_date(req_start_date)
            req_end_date = ms_to_date(req_end_date)
        params = dict()
        params['start'] = True
        params['min_date'] = datetime.strptime('2010-01-01', '%Y-%m-%d')
        params['max_date'] = datetime.strptime('2010-01-02', '%Y-%m-%d')
        params['end'] = True
        params['in_memory'] = False
        # convert dates from ms to datetime
        # start_date = ms_to_date(start_date)
        #end_date = ms_to_date(end_date)
        if len(df) > 0:
            params['min_date'], params['max_date'] = \
                dd.compute(df.block_timestamp.min(), df.block_timestamp.max())
            # check start
            logger.warning('start_date from compute:%s', params['min_date'])
            logger.warning('start from slider:%s', req_start_date)

            # set flag to true if data has to be fetched
            if req_start_date >= params['min_date']:
                    params['start'] = False
            if req_end_date <= params['max_date']:
                    params['end'] = False

            logger.warning('set_params_to_load:%s', params)
            # if table already in memory then set in-memory flag
            if params['start'] == False and params['end'] == False:
                params['in_memory'] = True
                return params
        logger.warning("in set_params_to_load:%s",params)
        return params

    except Exception:
        logger.error('set_params_loaded_params', exc_info=True)
        # if error set start date and end_date far in the past
        # this will trigger full cassandra load
        params['min_date'] = datetime.strptime('2010-01-01', '%Y-%m-%d')
        params['max_date'] = datetime.strptime('2010-01-02', '%Y-%m-%d')
        params['start'] = True
        params['end'] = True
        params['in_memory']=False
        return params

# delta is integer: +-
def get_relative_day(day,delta):
    if isinstance(day,str):
        day = datetime.strptime('%Y-%m-%d')
    elif isinstance(day,int):
        day = ms_to_date()
    day = day + datetime.timedelta(days=delta)
    day = datetime.strftime(day, '%Y-%m-%d')
    return day


def str_to_datetime(x):
    if isinstance(x, str):
        logger.warning("STR TO DATETIME CONVERSION:%s", x)
        return datetime.strptime(x, "%Y-%m-%d")
    return x

def datetime_to_str(x):
    if not isinstance(x, str):
        logger.warning(" DATETIME TO CONVERSION:%s", x)
        return datetime.strftime(x, "%Y-%m-%d")
    return x


# key_params: list of parameters to put in key
def compose_key(key_params, start_date,end_date):
    if isinstance(key_params,str):
        key_params = key_params.split(',')
    start_date = slider_ts_to_str(start_date)
    end_date = slider_ts_to_str(end_date)
    logger.warning('start_date in compose key:%s',start_date)
    key = ''
    for kp in key_params:
        if not isinstance(kp,str):
            kp = str(kp)
        key += kp+':'
    key = '{}{}:{}'.format(key,start_date, end_date)
    return key


# delta is integer: +-
def get_relative_day(day, delta):
    if isinstance(day, str):
        day = datetime.strptime('%Y-%m-%d')
    elif isinstance(day, int):
        day = ms_to_date(day)
    day = day + timedelta(days=delta)
    day = datetime.strftime(day, '%Y-%m-%d')
    return day

# small endian, starting with 0th index
def set_bit(value, bit):
    return value | (1 << bit)

# small endian starting with 0th index
def clear_bit(value, bit):
    return value & ~(1 << bit)

# small endian, starting with index 1
def isKthBitSet(n, k):
    if n & (1 << (k - 1)):
        return True
    else:
        return False

# append dask dataframes
def concat_dfs(top, bottom):
    top = top.repartition(npartitions=15)
    top = top.reset_index(drop=True)
    bottom = bottom.repartition(npartitions=15)
    bottom = bottom.reset_index(drop=True)
    top = dd.dataframe.concat([top,bottom])
    top = top.reset_index().set_index('index')
    top = top.drop_duplicates(keep='last')
    return top


# return the keys and flags for the data in redis and in cassandra
def set_construct_params(key_params, start_date, end_date, load_params):
    req_start_date = ms_to_date(start_date)
    req_end_date = ms_to_date(end_date)
    str_req_start_date = datetime.strftime(req_start_date, '%Y-%m-%d')
    str_req_end_date = datetime.strftime(req_end_date, '%Y-%m-%d')
    logger.warning('set_construct_params-req_start_date:%s', str_req_start_date)
    logger.warning('set_construct_params-req_end_date:%s', str_req_end_date)

    try:
        construct_params = dict()
        construct_params['load_type'] = 0
        construct_params['load_type'] = clear_bit(construct_params['load_type'], 1)
        construct_params['load_type'] = clear_bit(construct_params['load_type'], 3)
        construct_params['load_type'] = clear_bit(construct_params['load_type'], 7)

        construct_params['disk_storage_key_full'] = None
        construct_params['disk_storage_start_range'] = None
        construct_params['disk_storage_end_range'] = None
        construct_params['all_loaded'] = False

        if load_params['start'] and load_params['end']:
            # load entire frame
            construct_params['load_type'] = xor(construct_params['load_type'], LoadType.DISK_STORAGE_FULL.value)
            construct_params['disk_storage_key_full'] = [str_req_start_date, str_req_end_date]

        else:
            # if necessary load start from clickhouse
            if load_params['start']:
                loaded_day_before = get_relative_day(load_params['min_date'], -1)
                # set params OPTION 1
                construct_params['load_type'] = xor(construct_params['load_type'],
                                                    LoadType.DISK_STORAGE_START.value)
                construct_params['disk_storage_start_range'] = [str_req_start_date,
                                                                loaded_day_before]

            if load_params['end']:
                loaded_day_after = get_relative_day(load_params['min_date'], 1)
                # set params OPTION 1
                construct_params['load_type'] = xor(construct_params['load_type'],
                                                    LoadType.DISK_STORAGE_END.value)
                str_req_end_date = datetime.strftime(req_end_date, '%Y-%m-%d')
                construct_params['disk_storage_end_range'] = [loaded_day_after,
                                                              str_req_end_date]

        logger.warning("params result:%s", construct_params)

        return construct_params
    except Exception:
        logger.error('set load params:%s', exc_info=True)
        construct_params['load_type'] = xor(construct_params['load_type'], LoadType.DISK_STORAGE_FULL.value)
        construct_params['disk_storage_key_full'] = [str_req_start_date, str_req_end_date]
        # turn off all other bits
        construct_params['load_type'] = clear_bit(construct_params['load_type'], 0)
        construct_params['load_type'] = clear_bit(construct_params['load_type'], 3)
        construct_params['load_type'] = clear_bit(construct_params['load_type'], 7)

        construct_params['disk_storage_start_range'] = None
        construct_params['disk_storage_end_range'] = None
        return construct_params


# get the data differential from the required start range
def construct_warehouse_from_parquet_and_df(df, table,key_tab,cols, dedup_cols, req_start_date,
                                            req_end_date, load_params, cass_or_ch='clickhouse'):

    pq = PythonParquet()
    counter = 0

    if df is not None:
        if len(df) > 0:
            logger.warning("df original, TAIL:%s", df.tail())

    try:
        meta = ('block_timestamp','datetime64[ns]')
        # get the data parameters to determine from whence to load
        construct_params = set_construct_params(table, req_start_date,
                                                req_end_date, load_params)
        # load all from redis
        logger.warning('construct df, params:%s', construct_params)
        # load entire warehouse for parquet
        if 'warehouse' in table:
            key_params = [table]
        else:
            key_params = [table, key_tab]

        logger.warning("KEY_PARAMS IN :%s",key_params)
        if construct_params['load_type'] & LoadType.DISK_STORAGE_FULL.value \
                == LoadType.DISK_STORAGE_FULL.value:
            df = pq.load(key_params,req_start_date,req_end_date)
            counter += 2
            # convert redis datetime from string to date
        else:
            # add dsk start
            df_start = None
            df_end = None
            if construct_params['load_type'] & LoadType.DISK_STORAGE_START.value \
                    == LoadType.DISK_STORAGE_START.value:
                lst = construct_params['disk_storage_start_range']
                sdate = date_to_ms(lst[-2])
                edate = date_to_ms(lst[-1])
                df_start = pq.load(key_params, sdate, edate)

            if construct_params['load_type'] & LoadType.DISK_STORAGE_END.value \
                    == LoadType.DISK_STORAGE_END.value:
                lst = construct_params['disk_storage_end_range']
                sdate = date_to_ms(lst[0])
                edate = date_to_ms(lst[1])
                df_end = pq.load(key_params, sdate, edate)


            logger.warning("CONSTRUCT DF before APPENDS, TAIL:%s", df.tail(10))
            # concatenate end and start to original df
            if df_start is not None:
                #logger.warning("DF START, TAIL:%s", df_start.tail(5))
                df = concat_dfs(df_start,df)
                # turn off the start load_params
                construct_params['load_type'] = clear_bit(construct_params['load_type'], 0)
                construct_params['load_type'] = clear_bit(construct_params['load_type'], 3)
                construct_params['disk_storage_key_full'] = []
                construct_params['disk_storage_start_range'] = []
                counter += 1

            if df_end is not None:
                #logger.warning("DF END, TAIL:%s", df_end.head(5))
                df = concat_dfs(df, df_end)
                construct_params['load_type'] = clear_bit(construct_params['load_type'], 0)
                construct_params['load_type'] = clear_bit(construct_params['load_type'], 7)
                construct_params['disk_storage_key_full'] = []
                construct_params['disk_storage_end_range'] = []
                counter += 1

        if counter >= 2:
            construct_params['all_loaded'] = True


            # CLEAN UP
            check_to_drop = ['level_0']
            to_drop = []
            for value in check_to_drop:
                if value in df.columns.tolist():
                    to_drop.append(value)
            if len(to_drop) > 0:
                df = df.drop(to_drop,axis=1)

            logger.warning("END OF CONSTRUCTION, TAIL:%s", df.tail())

            del df_start
            del df_end

            gc.collect()

        logger.warning("df constructed, HEAD:%s", df.head())

        return df, construct_params
    except Exception:
        logger.error('set load params:%s', exc_info=True)


# get the data differential from the required start range
def construct_df_upon_load(df, table,key_tab,cols, dedup_cols, req_start_date,
                           req_end_date, load_params, cass_or_ch='clickhouse'):

    ch = PythonClickhouse('aion')

    if df is not None:
        if len(df) > 0:
            logger.warning("df original, TAIL:%s", df.tail())

    try:
        meta = ('block_timestamp','datetime64[ns]')
        # get the data parameters to determine from whence to load
        construct_params = set_construct_params([table,key_tab], req_start_date,
                                                req_end_date, load_params)
        # load all from redis
        logger.warning('construct df, params:%s', construct_params)
        if construct_params['load_type'] & LoadType.DISK_STORAGE_FULL.value \
                == LoadType.DISK_STORAGE_FULL.value:
            lst = construct_params['disk_storage_key_full']
            sdate = str_to_datetime(lst[-2])
            edate = str_to_datetime(lst[-1])
            df = ch.load_data(table,cols,sdate, edate)

            # convert redis datetime from string to date
            logger.warning("LOAD FROM clickhouse, TAIL:%s", df.tail())
        else:
            # add dsk start
            df_start = SD(table,cols,dedup_cols).get_df()
            df_end = SD(table,cols,dedup_cols).get_df()
            if construct_params['load_type'] & LoadType.DISK_STORAGE_START.value \
                    == LoadType.DISK_STORAGE_START.value:
                lst = construct_params['disk_storage_start_range']
                sdate = date_to_ms(lst[-2])
                edate = date_to_ms(lst[-1])
                df_start = ch.load_data(table, cols, sdate, edate)

            if construct_params['load_type'] & LoadType.DISK_STORAGE_END.value \
                    == LoadType.DISK_STORAGE_END.value:
                lst = construct_params['disk_storage_end_range']
                sdate = date_to_ms(lst[0])
                edate = date_to_ms(lst[1])
                df_end = ch.load_data(table, cols, sdate, edate)

            logger.warning("CONSTRUCT DF before APPENDS, TAIL:%s", df.tail(10))
            # concatenate end and start to original df
            if len(df_start)>0:
                logger.warning("DF START, TAIL:%s", df_start.tail(5))
                df = concat_dfs(df_start,df)
            if len(df_end)>0:
                logger.warning("DF END, TAIL:%s", df_end.start(5))
                df = concat_dfs(df, df_end)


            # CLEAN UP
            check_to_drop = ['level_0','index']
            to_drop = []
            for value in check_to_drop:
                if value in df.columns.tolist():
                    to_drop.append(value)
            if len(to_drop) > 0:
                df = df.drop(to_drop,axis=1)

            logger.warning("END OF CONSTRUCTION, TAIL:%s", df.tail())

            del df_start
            del df_end

            gc.collect()

        logger.warning("df constructed, HEAD:%s", df.head())

        return df

    except Exception:
        logger.error('construct df from load', exc_info=True)


def datetime_to_date(x):
    if isinstance(x, datetime):
        x = x.date()
    return x