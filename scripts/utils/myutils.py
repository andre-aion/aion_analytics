from scripts.utils.mylogger import mylogger
from scripts.utils.pythonRedis import LoadType, RedisStorage
from scripts.streaming.streamingBlock import Block

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
import gc

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
        to_check = tuple(range(0, 50000))
        qry ="""SELECT block_number, difficulty, block_date, 
            block_time, miner_addr FROM block
            WHERE block_number in """+str(to_check)

        df = pd.DataFrame(list(pc.session.execute(qry)))
        df = dd.dataframe.from_pandas(df, npartitions=15)
        logger.warning('%s',df.head(5))
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

# date time to ms
def date_to_ms(ts):
    if isinstance(ts,str):
        ts = datetime.strptime(ts,'%Y-%m-%d')
    ts = int(ts.timestamp())
    return ts

# convert date format for building cassandra queries
def date_to_cass_ts(ts):
    #ts = pd.Timestamp(ts, unit='ns')
    if isinstance(ts, str):
        ts = datetime.strptime(ts,'%Y-%m-%d')
    ts = int(ts.timestamp()*1000)
    return ts


#convert ms to string date
def slider_ts_to_str(ts):
    # convert to datetime if necessary
    if isinstance(ts,int) == True:
        ts = ms_to_date(ts)

    ts = datetime.strftime(ts,'%Y-%m-%d')
    return ts


# cols are a list
def construct_read_query(table,cols,startdate,enddate):
    qry = 'select '
    if len(cols) >= 1:
        for pos, col in enumerate(cols):
            if pos > 0:
                qry += ','
            qry += col
    else:
        qry += '*'

    qry += """ from {} where block_timestamp >={} and 
        block_timestamp <={} ALLOW FILTERING"""\
        .format(table, startdate, enddate)


    logger.warning('query:%s',qry)
    return qry

# dates are in milliseconds from sliders
def cass_load_from_daterange(pc, table, cols, from_date, to_date):
    try:
        if isinstance(from_date,int) == True:
            # convert ms from slider to nano for cassandra
            from_date = from_date
            to_date = to_date
        else:
            # convert from datetime to ns
            from_date = date_to_cass_ts(from_date)
            to_date = date_to_cass_ts(to_date)

        # construct query
        qry = construct_read_query(table, cols,
                                   from_date,
                                   to_date)
        df = pd.DataFrame(list(pc.session.execute(qry)))
        df = dd.dataframe.from_pandas(df, npartitions=15)
        logger.warning('data loaded from daterange :%s', df.tail(5))
        return df

    except Exception:
        logger.error('load from daterange', exc_info=True)

# check to see if the current data is within the active dataset
def set_loaded_params(df, start_date, end_date):
    try:
        params = dict()
        params['start'] = False
        params['min_date'] = None
        params['end'] = False
        params['max_date'] = None
        # convert dates from ms to datetime
        start_date = ms_to_date(start_date)
        end_date = ms_to_date(end_date)

        if len(df) > 0:
            params['min_date'], params['max_date'] = \
                dd.compute(df.block_date.min(),
                           df.block_date.max())
            logger.warning('set_loaded_params:%s',params)
            # check start
            if start_date > params['min_date']:
                params['start'] = True

            # check end
            if end_date < params['max_date']:
                params['end'] = True
        else:
            # if no df in memory set start date and end_date far in the past
            # this will trigger full cassandra load
            params['min_date'] = date_to_ms('2010-01-01')
            params['max_date'] = date_to_ms('2010-01-02')
            params['start'] = True
            params['end'] = True

        return params
    except Exception:
        logger.error('check loaded dates', exc_info=True)

# delta is integer: +-
def get_relative_day(day,delta):
    if isinstance(day,str):
        day = datetime.strptime('%Y-%m-%d')
    elif isinstance(day,int):
        day = ms_to_date()
    day = day + datetime.timedelta(days=delta)
    day = datetime.strftime(day, '%Y-%m-%d')
    return day


# get the data differential from the required start range
def construct_df_upon_load(pc, df, table, cols, req_start_date,
                           req_end_date,load_params):
    try:
        redis = RedisStorage()
        r = redis.conn
        # get the data parameters to determine from whence to load
        params = redis.set_load_params(table, req_start_date,
                                       req_end_date, load_params)
        # load all from redis
        logger.warning('construct df:%s',LoadType.REDIS_FULL.value)
        if params['load_type'] & LoadType.REDIS_FULL.value == LoadType.REDIS_FULL.value:
            sdate = datetime.strptime(load_params['redis_key_full'][0])
            edate = datetime.strptime(load_params['redis_key_full'][1])

            df = r.load_df(load_params['redis_full'], table, sdate, edate)
        # load all from cassandra
        elif params['load_type'] & LoadType.CASS_FULL.value == LoadType.CASS_FULL.value:
            sdate = date_to_cass_ts(req_start_date)
            edate = date_to_cass_ts(req_end_date)
            df = cass_load_from_daterange(pc, table, cols, sdate, edate)

        # load from both cassandra and redis
        else:
            # load start
            df_temp = dict()

            if params['load_type'] & LoadType.START_REDIS.value == LoadType.START_REDIS.value:
                sdate = datetime.strptime(load_params['redis_start_range'][0])
                edate = datetime.strptime(load_params['redis_start_range'][1])
                df_temp['redis'] = r.load_df(load_params['redis_key_start'], table, sdate, edate)

            if params['load_type'] & LoadType.REDIS_START_ONLY.value != LoadType.REDIS_START_ONLY.value:
                if params['load_type'] & LoadType.START_CASS.value == LoadType.START_CASS.value:
                    sdate = date_to_cass_ts(load_params['cass_end_range'][0])
                    edate = date_to_cass_ts(load_params['cass_end_range'][1])
                    df_temp['cass'] = cass_load_from_daterange(pc, table, cols, sdate, edate)

                    if params['load_type'] & LoadType.START_REDIS.value == LoadType.START_REDIS.value:
                        df_temp['cass'] = df_temp['cass'].append(df_temp['redis'], ignore_index=True)
                df = df_temp['cass'].append(df, ignore_index=True)

            else:
                df = df_temp['redis'].append(df, ignore_index=True)

            del df_temp
            gc.collect()

            # load end
            df_temp = dict()

            if params['load_type'] & LoadType.END_REDIS.value == LoadType.END_REDIS.value:
                sdate = datetime.strptime(load_params['redis_end_range'][0])
                edate = datetime.strptime(load_params['redis_end_range'][1])
                df_temp['redis'] = r.load_df(load_params['redis_key_end'], table, sdate, edate)

            if params['load_type'] & LoadType.REDIS_END_ONLY.value != LoadType.REDIS_END_ONLY.value:
                if params['load_type'] & LoadType.END_CASS.value == LoadType.END_CASS.value:
                    sdate = date_to_cass_ts(load_params['cass_end_range'][0])
                    edate = date_to_cass_ts(load_params['cass_end_range'][1])
                    df_temp['cass'] = cass_load_from_daterange(pc, table, cols, sdate, edate)

                    if params['load_type'] & LoadType.END_REDIS.value == LoadType.END_REDIS.value:
                        df_temp['redis'] = df_temp['redis'].append(df_temp['cass'], ignore_index=True)
                df = df.append(df_temp['redis'], ignore_index=True)

            else:
                df = df.append(df_temp['redis'], ignore_index=True)

            del df_temp
            gc.collect()


        # save df to  redis
        r.save_df(df, table, req_start_date, req_end_date)

        gc.collect()
        return df

    except Exception:
        logger.error('construct df from load', exc_info=True)