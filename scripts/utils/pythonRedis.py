from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import ms_to_str, ms_to_date, \
    get_relative_day
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


logger = mylogger(__file__)
EXPIRATION_SECONDS = 86400*4

class LoadType(Enum):
    REDIS_FULL = bin(1)
    CASS_FULL = bin(2)
    START_REDIS = bin(4)
    START_CASS = bin(8)
    END_REDIS = bin(16)
    END_CASS = bin(32)
    REDIS_END_ONLY=bin(64)
    REDIS_START_ONLY=bin(128)


class RedisStorage:
    conn = redis.StrictRedis(
        host='localhost',
        port=6379)

    def __init__(self):
        pass

    def compose_key(self,table, start_date,end_date):
        start_date = ms_to_str(start_date)
        end_date = ms_to_str(end_date)
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


    # if the data frame is found in redis return the key
    def get_load_params(self, table, req_start_date, req_end_date,
                      load_flags):
        # get keys
        str_to_match = '*'+table+'*'
        matches = self.conn.scan_iter(match=str_to_match)
        # 10000: cass end
        flags = dict()
        flags['load_type'] = 0
        flags['redis_key_full'] = None
        flags['redis_key_start'] = None
        flags['redis_key_end'] = None
        flags['redis_start_range'] = None
        flags['cass_start_range'] = None
        flags['redis_end_range'] = None
        flags['cass_end_range'] = None


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
                    flags['redis_key_full'] = match
                    flags['load_type'] = LoadType.FULL
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
                    flags['load_type'] = xor(flags, LoadType.CASS_FULL)

                else:
                    # the loaded start date is greater than the required start date
                    if load_flags['start']:
                        # if redis only is set, then stop do not check again
                        if flags['load_type'] & LoadType.REDIS_START_ONLY != LoadType.REDIS_START_ONLY:
                            loaded_day_before = get_relative_day(load_flags['min_date'], -1)
                            if load_flags['min_date'] > key_start_date:
                                """
                                loaded_df           |-------------------------
                                redis_df     ||--------------------- 
                                """
                                flags['redis_key_start'] = match
                                flags['load_type'] = xor(flags['load_type'], LoadType.START_REDIS_ONLY)
                                flags['load_type'] = xor(flags['load_type'], LoadType.START_REDIS)
                                flags['redis_start_range'] = [lst[2], loaded_day_before]

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

                                #set flags OPTION 1
                                flags['load_type'] = flags['load_type'] + LoadType.START_CASS
                                str_req_start_date = datetime.strftime(req_start_date,'%Y-%m-%d')
                                str_req_end_date = datetime.strftime(req_end_date,'%Y-%m-%d')
                                flags['cass_start_range'] = [str_req_start_date,loaded_day_before]

                                # find out data range to load from cass, and also from redis
                                str_loaded_min_date = datetime.strftime(load_flags['min_date'], '%Y-%m-%d')
                                if load_flags['min_date'] > key_start_date:  # OPTION 2
                                    # set start date range to get from redis
                                    """
                                    required ||| ----------------------
                                    redis_df       || ----------------------------
                                    loaded_df            | ---------------------
                                    """
                                    # set start date range to get from redis and cassandra
                                    # set flags
                                    flags['redis_key_start'] = match
                                    flags['load_type'] = xor(flags, LoadType.START_REDIS)
                                    flags['redis_start_range'] = [lst[1], loaded_day_before]
                                    # set/adjust parameters for grabbing redis and cass data
                                    key_day_before = get_relative_day(lst[1], -1)
                                    flags['cass_start_range'] = [str_req_start_date, key_day_before]


                    # if the required load date is more than the required start date
                    if load_flags['end']:
                        # if the end only needs to be loaded from reddis, then never repeat this step
                        if flags['load_type'] & LoadType.REDIS_END_ONLY != LoadType.REDIS_END_ONLY:
                            loaded_day_after = get_relative_day(load_flags['min_date'], 1)
                            if load_flags['max_date'] < key_end_date:
                                """
                                loaded_df    ------------------------- ||
                                redis_df                   --------------------- |
                                """
                                flags['redis_key_end'] = match
                                flags['load_type'] = xor(flags['load_type'], LoadType.REDIS_END_ONLY)
                                flags['load_type'] = xor(flags['load_type'], LoadType.END_REDIS)
                                flags['redis_end_range'] = [lst[2], loaded_day_after]

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
                                # set flags OPTION 1
                                flags['load_type'] = flags['load_type'] + LoadType.END_CASS
                                str_req_end_date = datetime.strftime(req_end_date, '%Y-%m-%d')
                                flags['cass_end_range'] = [loaded_day_after, str_req_end_date]
                                if load_flags['max_date'] < key_end_date:
                                    """
                                    required         -----------------------------|||
                                    redis_df     -------------------------||
                                    loaded_df         ------------|
                                    """
                                    # set flags
                                    flags['redis_key_end'] = match
                                    flags['load_type'] = xor(flags, LoadType.END_REDIS)
                                    flags['redis_end_range'] = [lst[2], loaded_day_after]
                                    # set/adjust parameters for grabbing redis and cass data
                                    key_day_after = get_relative_day(lst[2], 1)
                                    flags['cass_end_range'] = [key_day_after, str_req_end_date]

        logger.warning("retrieve data from redis:%s", flags)

        return flags

    
