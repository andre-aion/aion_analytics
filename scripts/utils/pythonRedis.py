from scripts.utils.mylogger import mylogger
import redis
import pickle
import redis
import zlib
import pandas as pd
import dask as dd
from tornado.gen import coroutine
from datetime import datetime
from enum import Enum
from operator import xor
import numpy as np


logger = mylogger(__file__)
EXPIRATION_SECONDS = 86400*4 #retain for 4 days in redis


class LoadType(Enum):
    REDIS_FULL = 1
    CASS_FULL = 2
    START_REDIS = 4
    START_CASS = 8
    END_REDIS = 16
    END_CASS = 32
    REDIS_END_ONLY = 64
    REDIS_START_ONLY = 128


class RedisStorage:
    conn = redis.StrictRedis(
        host='localhost',
        port=6379)

    def __init__(self):
        pass

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

                logger.warning('from ms_to_date: %s', ts)
            return ts
        except Exception:
            logger.error('ms_to_date', exc_info=True)
            return ts

    # delta is integer: +-
    def get_relative_day(self, day, delta):
        if isinstance(day, str):
            day = datetime.strptime('%Y-%m-%d')
        elif isinstance(day, int):
            day = self.ms_to_date()
        day = day + datetime.timedelta(days=delta)
        day = datetime.strftime(day, '%Y-%m-%d')
        return day

    # convert ms to string date
    def slider_ts_to_str(self,ts):
        # convert to datetime if necessary
        if isinstance(ts, int) == True:
            ts = self.ms_to_date(ts)

        ts = datetime.strftime(ts, '%Y-%m-%d')
        return ts

    def compose_key(self,table, start_date,end_date):
        start_date = self.slider_ts_to_str(start_date)
        end_date = self.slider_ts_to_str(end_date)
        key = '{}:{}:{}'.format(table, start_date, end_date)
        return key

    @coroutine
    def save_df(self, df, table, start_date, end_date):
        try:
            #convert dates to strings
            key = self.compose_key(table, start_date, end_date)
            self.conn.set(key, EXPIRATION_SECONDS,
                            zlib.compress(pickle.dumps(df)))
            logger.warning("%s saved to reddis",key)
        except Exception:
            logger.error('save df',exc_info=True)


    def load_df(self, key, table, start_date, end_date):
        try:
            df = pickle.loads(self.conn.get(key))
            # filter using start date and end date
            df = df[(df.block_date >= start_date)
                    and (df.block_date <= end_date)]
            return df
        except Exception:
            logger.error('load df', exc_info=True)

    # small endian, starting with index 1
    def isKthBitSet(self,n, k):
        if n & (1 << (k - 1)):
            return True
        else:
            return False

    # small endian, starting with 0th index
    def set_bit(value, bit):
        return value | (1 << bit)

    # small endian starting with 0th index
    def clear_bit(value, bit):
        return value & ~(1 << bit)

    # return the keys and flags for the data in redis and in cassandra
    def set_load_params(self, table, req_start_date, req_end_date,
                        load_params):
        try:

            # get keys
            str_to_match = '*'+table+'*'
            matches = self.conn.scan_iter(match=str_to_match)
            # 10000: cass end
            params = dict()
            params['load_type'] = 0
            params['redis_key_full'] = None
            params['redis_key_start'] = None
            params['redis_key_end'] = None
            params['redis_start_range'] = None
            params['cass_start_range'] = None
            params['redis_end_range'] = None
            params['cass_end_range'] = None


            if matches:
                for match in matches:
                    lst = match.split(':')
                    #convert start date in key to datetime
                    key_start_date = datetime.strptime(lst[1],'%Y-%m-%d')
                    key_end_date = datetime.strptime(lst[2],'%Y-%m-%d')
                    # check to see if there is any data to be retrieved from reddis
                    if req_start_date <= key_start_date and req_end_date >= key_end_date:
                        """
                        redis_df || ---------------------------- ||
                        required     | --------------------- |
                        """

                        logger.warning("Full match found: Redis data frame overlaps")
                        params['redis_key_full'] = match
                        params['load_type'] = LoadType.FULL.value
                        # turn cass full off if it is set
                        if self.isKthBitSet(params['load_type'],2):
                            self.clear_bit(1)

                        break
                    elif req_end_date > key_start_date or req_start_date < key_start_date:
                        """
                        redis_df                    || ---------------- ||
                        required   | ---------- |
                                   OR
                        redis_df || --------- ||
                        required                   | --------------------- |
    
                        """
                        logger.warning("no overlay, retrieve ENTIRE df from casssandra")
                        params['load_type'] = xor(params['load_type'], LoadType.CASS_FULL.value)

                    else:
                        # turn cass full off if it is set
                        if self.isKthBitSet(params['load_type'], 2):
                            self.clear_bit(1)

                        # the loaded start date is greater than the required start date
                        if load_params['start']:
                            # if redis only is set, then stop do not check again

                            if params['load_type'] & LoadType.REDIS_START_ONLY.value != LoadType.REDIS_START_ONLY.value:
                                loaded_day_before = self.get_relative_day(load_params['min_date'], -1)
                                if load_params['min_date'] > key_start_date:
                                    """
                                    loaded_df           |-------------------------
                                    redis_df     ||--------------------- 
                                    """
                                    params['redis_key_start'] = match
                                    params['load_type'] = xor(params['load_type'], LoadType.REDIS_START_ONLY.value)
                                    params['load_type'] = xor(params['load_type'], LoadType.START_REDIS.value)
                                    params['redis_start_range'] = [lst[2], loaded_day_before]

                                else:

                                    # retrieve data from cassandra datastore
                                    """
                                    OPTION 2:
                                    redis_df          || ----------------------------
                                    required     | ---------------------
                                    
                                               OR
                                    OPTION: 1
                                    redis_df    || ----------------------------
                                    required                | -------------------------
                                    
                                    """
                                    logger.warning("retrieve START data from cassandra")

                                    #set params OPTION 1
                                    params['load_type'] = xor(params['load_type'], LoadType.START_CASS.value)
                                    str_req_start_date = datetime.strftime(req_start_date,'%Y-%m-%d')
                                    str_req_end_date = datetime.strftime(req_end_date,'%Y-%m-%d')
                                    params['cass_start_range'] = [str_req_start_date,loaded_day_before]

                                    # find out data range to load from cass, and also from redis
                                    str_loaded_min_date = datetime.strftime(load_params['min_date'], '%Y-%m-%d')
                                    if load_params['min_date'] > key_start_date:  # OPTION 2
                                        # set start date range to get from redis
                                        """
                                        required ||| ----------------------
                                        redis_df       || ----------------------------
                                        loaded_df            | ---------------------
                                        """
                                        # set start date range to get from redis and cassandra
                                        # set params
                                        params['redis_key_start'] = match
                                        params['load_type'] = xor(params, LoadType.START_REDIS.value)
                                        params['redis_start_range'] = [lst[1], loaded_day_before]
                                        # set/adjust parameters for grabbing redis and cass data
                                        key_day_before = self.get_relative_day(lst[1], -1)
                                        params['cass_start_range'] = [str_req_start_date, key_day_before]


                        # if the required load date is more than the required start date
                        if load_params['end']:
                            # if the end only needs to be loaded from reddis, then never repeat this step
                            if params['load_type'] & LoadType.REDIS_END_ONLY.value != LoadType.REDIS_END_ONLY.value:
                                loaded_day_after = self.get_relative_day(load_params['min_date'], 1)
                                if load_params['max_date'] < key_end_date:
                                    """
                                    loaded_df    ------------------------- ||
                                    redis_df                   --------------------- |
                                    """
                                    params['redis_key_end'] = match
                                    params['load_type'] = xor(params['load_type'], LoadType.REDIS_END_ONLY.value)
                                    params['load_type'] = xor(params['load_type'], LoadType.END_REDIS.value)
                                    params['redis_end_range'] = [lst[2], loaded_day_after]

                                else:

                                    """
                                    OPTION 2:
                                    redis_df    ------------------------- ||
                                    required                   --------------------- |
            
                                               OR
                                    OPTION 1:
                                    redis_df    ---------------------------- ||
                                    required                ----------- |
            
                            
                                    """
                                    # set params OPTION 1
                                    params['load_type'] = xor(params['load_type'], LoadType.END_CASS.value)
                                    str_req_end_date = datetime.strftime(req_end_date, '%Y-%m-%d')
                                    params['cass_end_range'] = [loaded_day_after, str_req_end_date]
                                    if load_params['max_date'] < key_end_date:
                                        """
                                        required         -----------------------------|||
                                        redis_df     -------------------------||
                                        loaded_df         ------------|
                                        """
                                        # set params
                                        params['redis_key_end'] = match
                                        params['load_type'] = xor(params, LoadType.END_REDIS.value)
                                        params['redis_end_range'] = [lst[2], loaded_day_after]
                                        # set/adjust parameters for grabbing redis and cass data
                                        key_day_after = self.get_relative_day(lst[2], 1)
                                        params['cass_end_range'] = [key_day_after, str_req_end_date]

            logger.warning("retrieve data from redis:%s", params)

            return params
        except Exception:
            logger.error('set load params',exc_info=True)