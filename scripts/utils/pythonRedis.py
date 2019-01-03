from scripts.utils.mylogger import mylogger
import redis
import pickle
import redis
import zlib
import pandas as pd
import dask as dd
import dask as dd
from tornado.gen import coroutine
from datetime import datetime, timedelta
from enum import Enum
from operator import xor
import numpy as np
import cloudpickle
import msgpack


logger = mylogger(__file__)
EXPIRATION_SECONDS = 86400*4 #retain for 4 days in redis


class LoadType(Enum):
    REDIS_FULL = 1
    DISK_STORAGE_FULL = 2
    REDIS_START = 4
    START_DISK_STORAGE = 8
    REDIS_END = 16
    DISK_STORAGE_END = 32
    REDIS_END_ONLY = 64
    REDIS_START_ONLY = 128


class RedisStorage:
    conn = redis.StrictRedis(
        host='localhost',
        port=6379)

    def __init__(self):
        pass

    # convert dates from timestamp[ms] to datetime[ns]
    def ms_to_date(self, ts, precision='s'):
        try:
            if isinstance(ts, int):
                # change milli to seconds
                if ts > 16307632000:
                    ts = ts // 1000
                if precision == 'ns':
                    ts = datetime.utcfromtimestamp(ts)
                    # convert to nanosecond representation
                    ts = np.datetime64(ts).astype(datetime)
                    ts = pd.Timestamp(datetime.date(ts))
                elif precision == 's':# convert to ms respresentation
                    ts = datetime.fromtimestamp(ts)

            elif isinstance(ts, datetime):
                return ts
                logger.warning('ms_to_date: %s', ts)
            return ts
        except Exception:
            logger.error('ms_to_date', exc_info=True)
            return ts

    # delta is integer: +-
    def get_relative_day(self, day, delta):
        if isinstance(day, str):
            day = datetime.strptime('%Y-%m-%d')
        elif isinstance(day, int):
            day = self.ms_to_date(day)
        day = day + timedelta(days=delta)
        day = datetime.strftime(day, '%Y-%m-%d')
        return day

    # convert ms to string date
    def datetime_or_ts_to_str(self, ts):
        if isinstance(ts,str) == False:
            # convert to datetime if necessary
            ts = self.ms_to_date(ts)
            ts = datetime.strftime(ts, '%Y-%m-%d')
        return ts

    # key_params: list of parameters to put in key
    def compose_key(self,key_params, start_date,end_date):
        if isinstance(key_params,str):
            key_params = key_params.split(',')
        start_date = self.datetime_or_ts_to_str(start_date)
        end_date = self.datetime_or_ts_to_str(end_date)
        logger.warning('start_date in compose key:%s',start_date)
        key = ''
        for kp in key_params:
            if not isinstance(kp,str):
                kp = str(kp)
            key += kp+':'
        key = '{}{}:{}'.format(key,start_date, end_date)
        return key

    @coroutine
    def save(self,item, key_params, start_date, end_date):
        try:
            #convert dates to strings
            item = item.compute()
            key = self.compose_key(key_params, start_date, end_date)
            self.conn.setex(name=key, time=EXPIRATION_SECONDS,
                            value=zlib.compress(pickle.dumps(item)))
            logger.warning("SAVE: %s saved to redis",key)
        except Exception:
            logger.error('save to redis',exc_info=True)


    def load(self, key_params, start_date, end_date, key=None, item_type=''):
        try:
            if key is None:
                start_date = self.datetime_or_ts_to_str(start_date)
                end_date = self.datetime_or_ts_to_str(end_date)

                key = self.compose_key(key_params,start_date,end_date)

            logger.warning('load-df key:%s', key)
            item = pickle.loads(zlib.decompress(self.conn.get(key)))
            logger.warning("from redis load:%s",item)

            return item
        except Exception:
            logger.error('load item', exc_info=True)


    # small endian, starting with index 1
    def isKthBitSet(self,n, k):
        if n & (1 << (k - 1)):
            return True
        else:
            return False

    # small endian, starting with 0th index
    def set_bit(self, value, bit):
        return value | (1 << bit)

    # small endian starting with 0th index
    def clear_bit(self, value, bit):
        return value & ~(1 << bit)


    # return the keys and flags for the data in redis and in cassandra
    def set_construct_params(self, table, start_date, end_date, load_params):
        req_start_date = self.ms_to_date(start_date)
        req_end_date = self.ms_to_date(end_date)
        str_req_start_date = datetime.strftime(req_start_date, '%Y-%m-%d')
        str_req_end_date = datetime.strftime(req_end_date, '%Y-%m-%d')
        logger.warning('set_construct_params-req_start_date:%s', str_req_start_date)
        logger.warning('set_construct_params-req_end_date:%s', str_req_end_date)

        try:
            # get keys
            str_to_match = '*'+table+':*'
            matches = self.conn.scan_iter(match=str_to_match)
            # 10000: cass end
            construct_params = dict()
            construct_params['load_type'] = 0
            construct_params['redis_key_full'] = None
            construct_params['disk_storage_key_full'] = None
            construct_params['redis_key_start'] = None
            construct_params['redis_key_end'] = None
            construct_params['redis_start_range'] = None
            construct_params['disk_storage_start_range'] = None
            construct_params['redis_end_range'] = None
            construct_params['disk_storage_end_range'] = None
            construct_params['redis_keys_to_delete'] = []

            if matches:
                for redis_key in matches:
                    redis_key_encoded = redis_key
                    redis_key = str(redis_key,'utf-8')
                    logger.warning('redis_key:%s', redis_key)

                    redis_key_list = redis_key.split(':')
                    logger.warning('matching keys :%s', redis_key_list)
                    #convert start date in key to datetime
                    key_start_date = datetime.strptime(redis_key_list[-2],'%Y-%m-%d')
                    key_end_date = datetime.strptime(redis_key_list[-1],'%Y-%m-%d')

                    # check to see if there is all data to be retrieved from reddis
                    logger.warning('req_start_date:%s', req_start_date)
                    logger.warning('key_start_date:%s', key_start_date)

                    # matches to delete: make a list
                    if req_start_date <= key_start_date and req_end_date >= key_end_date:
                        """
                        redis_df      || ---------------- ||
                        required  | --------------------------- |

                        """
                        # only add keys that are not exactly matched
                        if req_start_date != key_start_date and req_end_date != key_end_date:
                            construct_params['redis_keys_to_delete'].append(redis_key_encoded)
                            logger.warning("REDIS KEY TO BE DELETED:%s",redis_key)


                    if req_start_date >= key_start_date and req_end_date <= key_end_date:
                        """
                        redis_df || ---------------------------- ||
                        required     | --------------------- |
                        """
                        logger.warning("Full redis_key found: Redis data frame overlaps")
                        construct_params['redis_key_full'] = redis_key
                        construct_params['load_type'] = LoadType.REDIS_FULL.value
                        # turn off all other bits and date ranges
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 1)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 3)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 7)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 15)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 31)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 63)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 127)

                        construct_params['disk_storage_full_range'] = None
                        construct_params['disk_storage_start_range'] = None
                        construct_params['disk_storage_end_range'] = None
                        construct_params['redis_start_range'] = None
                        construct_params['redis_end_range'] = None
                        construct_params['redis_key_end'] = None
                        construct_params['redis_key_start'] = None


                        break
                    elif req_end_date < key_start_date or req_start_date > key_end_date:
                        """
                        redis_df                    || ---------------- ||
                        required   | ---------- |
                                   OR
                        redis_df || --------- ||
                        required                   | --------------------- |
    
                        """
                        logger.warning("no overlay, retrieve ENTIRE df from casssandra")
                        construct_params['load_type'] = xor(construct_params['load_type'], LoadType.DISK_STORAGE_FULL.value)
                        construct_params['disk_storage_key_full'] = [str_req_start_date, str_req_end_date]
                        # turn off all other bits
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 0)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 3)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 7)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 15)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 31)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 63)
                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 127)

                    else:
                            # turn cass full off if it is set
                            construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 1)

                            # the loaded start date is greater than the required start date
                            if load_params['start']:
                                # if redis only is set, then do not check again
                                if construct_params['load_type'] & LoadType.REDIS_START_ONLY.value != LoadType.REDIS_START_ONLY.value:
                                    loaded_day_before = self.get_relative_day(load_params['min_date'], -1)
                                    if load_params['min_date'] > key_start_date:
                                        """
                                        loaded_df           |-------------------------
                                        redis_df     ||--------------------- 
                                        """
                                        # set redis values
                                        construct_params['redis_key_start'] = redis_key
                                        construct_params['redis_start_range'] = [redis_key_list[-2], loaded_day_before]
                                        construct_params['load_type'] = xor(construct_params['load_type'], LoadType.REDIS_START_ONLY.value)
                                        construct_params['load_type'] = xor(construct_params['load_type'], LoadType.REDIS_START.value)

                                        # turn off cass values
                                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 7) # clear start cass
                                        construct_params['disk_storage_start_range'] = None

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
                                        # IF START_REDIS IS SET DO NOT SET AGAIN, WE WILL WORRY ABOUT BEST SCENARIO
                                        # IN FUTURE MODELS

                                        if construct_params['load_type'] & LoadType.REDIS_START.value \
                                                != LoadType.REDIS_START.value:

                                            #set params OPTION 1
                                            construct_params['load_type'] = xor(construct_params['load_type'], LoadType.START_DISK_STORAGE.value)
                                            construct_params['disk_storage_start_range'] = [str_req_start_date,loaded_day_before]
                                            # turn off redis start if its on
                                            #params['load_type'] = self.clear_bit(params['load_type'], 3)

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
                                                construct_params['redis_key_start'] = redis_key
                                                construct_params['load_type'] = xor(construct_params, LoadType.REDIS_START.value)
                                                construct_params['redis_start_range'] = [redis_key_list[-2], loaded_day_before]
                                                # set/adjust parameters for grabbing redis and cass data
                                                key_day_before = self.get_relative_day(redis_key_list[-2], -1)
                                                construct_params['disk_storage_start_range'] = [str_req_start_date, key_day_before]


                            # if the required load date is more than the required start date
                            if load_params['end']:
                                # if the end only needs to be loaded from redis, then never repeat this step
                                if construct_params['load_type'] & LoadType.REDIS_END_ONLY.value != LoadType.REDIS_END_ONLY.value:
                                    loaded_day_after = self.get_relative_day(load_params['min_date'], 1)
                                    if load_params['max_date'] < key_end_date:
                                        """
                                        loaded_df    ------------------------- ||
                                        redis_df                   --------------------- |
                                        """
                                        construct_params['redis_key_end'] = redis_key
                                        construct_params['load_type'] = xor(construct_params['load_type'], LoadType.REDIS_END_ONLY.value)
                                        construct_params['load_type'] = xor(construct_params['load_type'], LoadType.REDIS_END.value)
                                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 7) # clear start cass
                                        construct_params['redis_end_range'] = [redis_key_list[-1], loaded_day_after]

                                        # turn off cass values
                                        construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 31)
                                        construct_params['disk_storage_start_range'] = None


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
                                        # IF START_REDIS IS SET DO NOT SET AGAIN, WE WILL WORRY ABOUT BEST SCENARIO
                                        # IN FUTURE MODELS

                                        if construct_params['load_type'] & LoadType.REDIS_START.value \
                                                != LoadType.REDIS_START.value:

                                            # set params OPTION 1
                                            construct_params['load_type'] = xor(construct_params['load_type'], LoadType.DISK_STORAGE_END.value)
                                            str_req_end_date = datetime.strftime(req_end_date, '%Y-%m-%d')
                                            construct_params['disk_storage_end_range'] = [loaded_day_after, str_req_end_date]
                                            if load_params['max_date'] < key_end_date:
                                                """
                                                required         -----------------------------|||
                                                redis_df     -------------------------||
                                                loaded_df         ------------|
                                                """
                                                # set params
                                                construct_params['redis_key_end'] = redis_key
                                                construct_params['load_type'] = xor(construct_params, LoadType.REDIS_END.value)
                                                construct_params['redis_end_range'] = [loaded_day_after, redis_key_list[-1]]
                                                # set/adjust parameters for grabbing redis and cass data
                                                key_day_after = self.get_relative_day(redis_key_list[-1], 1)
                                                construct_params['disk_storage_end_range'] = [key_day_after, str_req_end_date]

                # if no redis_key extant pull all from cassandra
                if construct_params['load_type'] == 0:
                    # set params for cass full range
                    construct_params['load_type'] = xor(construct_params['load_type'], LoadType.DISK_STORAGE_FULL.value)
                    construct_params['disk_storage_full_range'] = [req_start_date, req_end_date]


                    # turn off all other bits
                    construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 0)
                    construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 3)
                    construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 7)
                    construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 15)
                    construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 31)
                    construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 63)
                    construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 127)


            else:
                # set params for cass full range
                construct_params['load_type'] = xor(construct_params['load_type'], LoadType.DISK_STORAGE_FULL.value)
                construct_params['disk_storage_full_range'] = [req_start_date, req_end_date]

                # if no matches fallback is to pull all from cassandra
                construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 0)
                construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 3)
                construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 7)
                construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 15)
                construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 31)
                construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 63)
                construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 127)

                construct_params['disk_storage_start_range'] = None
                construct_params['disk_storage_end_range'] = None
                construct_params['redis_start_range'] = None
                construct_params['redis_end_range'] = None
                construct_params['redis_key_end'] = None
                construct_params['redis_key_start'] = None

            logger.warning("params result:%s", construct_params)

            return construct_params
        except Exception:
            logger.error('set load params:%s', exc_info=True)
            construct_params['load_type'] = xor(construct_params['load_type'], LoadType.DISK_STORAGE_FULL.value)
            construct_params['disk_storage_key_full'] = [str_req_start_date, str_req_end_date]
            # turn off all other bits
            construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 0)
            construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 3)
            construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 7)
            construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 15)
            construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 31)
            construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 63)
            construct_params['load_type'] = self.clear_bit(construct_params['load_type'], 127)

            construct_params['disk_storage_start_range'] = None
            construct_params['disk_storage_end_range'] = None
            construct_params['redis_start_range'] = None
            construct_params['redis_end_range'] = None
            construct_params['redis_key_end'] = None
            construct_params['redis_key_start'] = None
            return construct_params
