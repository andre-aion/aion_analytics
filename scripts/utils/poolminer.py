from tornado.gen import coroutine

from scripts.utils.mylogger import mylogger
from scripts.utils.pythonRedis import RedisStorage
import pandas as pd
import dask as dd
import gc

r = RedisStorage()
logger = mylogger(__file__)

def remove_quotes(x):
    x = x.replace("'","")
    return x

# explode into new line for each list member
def explode_transaction_hashes(df,column='transaction_hashes'):
    try:
        df = df[column].map_partitions(remove_quotes)
        return df[column].apply(pd.Series, 1).stack().reset_index(level=1, drop=True)
    except Exception:
        logger.error("explode transaction hashes", exc_info=True)

def get_key_in_redis(key_params,start_date,end_date):
    # get keys
    str_to_match = r.compose_key(key_params,start_date,end_date)
    matches = r.conn.scan_iter(match=str_to_match)
    redis_key = None
    if matches:
        for redis_key in matches:
            redis_key_encoded = redis_key
            redis_key = str(redis_key, 'utf-8')
            logger.warning('redis_key:%s', redis_key)
            break
    return redis_key

# make data warehouse only if tier1 and tier2 miner lists do not exits
def make_poolminer_warehouse(df_tx, df_block, start_date, end_date):
    warehouse = warehouse_needed(start_date,end_date)
    try:
        # first look for ware house in redis
        key_params = ['block_tx_warehouse']
        warehouse_key = get_key_in_redis(key_params, start_date, end_date)
        if warehouse_key is not None:
            df = r.load(key_params,start_date,end_date,key=warehouse_key,item_type='dataframe')
            return df
        else:
                df_block = explode_transaction_hashes(df_block)
                # join block and transaction table
                df = df_block.merge(df_tx, how='left',
                                              left_on='transaction_hashes',
                                              right_on='transaction_hash')  # do the merge\
                df = df.drop('transaction_hashes',axis=1)
                df = df.reset_index()
                df = df.compute()

                # save to redis
                r.save(df, key_params, start_date, end_date)
                return df
    except Exception:
        logger.error("make poolminer warehouse",exc_info=True)


def daily_percent(df,x):
    df_perc = df[(df.block_date== x)]
    total = df.block_number.sum()
    return 100*x.block_number/total

@coroutine
def list_by_tx_paid_out(df, delta_days, threshold=10):
    try:
        df_temp = df.groupBy('from_addr').agg({'to_addr': 'count'}).reset_index()
        # find daily mean
        df_temp = df_temp[df_temp.to_addr >= threshold*delta_days]
        lst = df_temp['to_addr'].unique().compute()
        logger.warning("miners found by paid out: %s",len(lst))
        del df_temp
        gc.collect()
        return lst
    except Exception:
        logger.error("tx paid out", exc_info=True)


@coroutine
def list_from_blocks_mined_daily(df,delta_days,threshold_percentage=1):
    try:
        df_temp = df.groupBy('miner_address').agg({'block_number': 'count'}).reset_index()
        total_blocks_mined_daily = 8640
        # convert percentage threshold to number
        threshold = threshold_percentage*delta_days*total_blocks_mined_daily/100
        df_temp = df_temp[df_temp.miner_address >= threshold]
        lst = df_temp['miner_address'].unique().compute()
        logger.warning("miners found by blocks mined: %s", len(lst))
        del df_temp
        gc.collect()
        return lst
    except Exception:
        logger.error("block mined daily", exc_info=True)


def warehouse_needed(start_date,end_date, threshold_tx_paid_out=10,threshold_blocks_mined_per_day=1,
                     threshold_tier2_pay_in=2):
    try:
        make_warehouse = True
        # look for both miner lists first
        # check to is if it is saved in redis
        key_params = ['tier1_miner_list', threshold_tx_paid_out,
                      threshold_blocks_mined_per_day]
        tier1_list = get_key_in_redis(key_params, start_date, end_date)
        if tier1_list is not None:
            # then look for tier2 list
            key_params = ['tier2_miner_list', threshold_tier2_pay_in]
            tier2_list = get_key_in_redis(key_params, start_date, end_date)
            if tier2_list is not None:
                # then simply return the two lists
                make_warehouse = False

        return make_warehouse
    except Exception:
        logger.error("warehouse needed", exc_info=True)


def make_tier1_list(df, start_date, end_date, threshold_tx_paid_out=10,
                    threshold_blocks_mined_per_day=1,):

    try:
        # check to is if it is saved in redis
        key_params = ['tier1_miner_list',threshold_tx_paid_out,
                      threshold_blocks_mined_per_day]
        redis_key = get_key_in_redis(key_params,start_date,end_date)

        # load data from redis if saved, else compose miner list
        if redis_key is not None:
            tier1_miner_list = r.load(key_params,start_date,end_date,
                                      key=redis_key,item_type='list')
        else:
            # Count transactions paid out per day: group transactions by date and miner
            # find min day find max day

            delta_days = (end_date - start_date).days
            if delta_days <= 0:
                delta_days = 1

            # tier 1 = percentage mined per day > threshold || transactions paid out > threshold per day#
            # make unique list of tier 1
            lst_a = yield list_by_tx_paid_out(df,delta_days,threshold_tx_paid_out)
            lst_b = yield list_from_blocks_mined_daily(df, delta_days,threshold_blocks_mined_per_day)
            # merge lists, drop duplicates
            tier1_miner_list = list(set(lst_a + lst_b))
            # save tier1 miner list to redis
            r.save(tier1_miner_list,key_params,start_date, end_date)

            del lst_a, lst_b
            gc.collect()

        return tier1_miner_list
    except Exception:
        logger.error("tier 1 miner list", exc_info=True)


def make_tier2_list(df,tier1_miner_list,
                    start_date, end_date,
                    threshold_tier2_pay_in=1,
                    threshold_tx_paid_out=10,
                    threshold_blocks_mined_per_day=1
                    ):
    try:
        # check to is if it is saved in redis
        key_params = ['tier2_miner_list',threshold_tier2_pay_in,
                      start_date, end_date]
        redis_key = get_key_in_redis(key_params,start_date,end_date)

        # load data from redis if saved, else compose miner list
        if redis_key is not None:
            tier2_miner_list = r.load(key_params,start_date,end_date,
                                      key=redis_key,item_type='list')
        else:
            tier1_miner_list = make_tier1_list(df,start_date,end_date,
                                               threshold_tx_paid_out,
                                               threshold_blocks_mined_per_day)
            # get or make warehouse
            # Count transactions paid out per day: group transactions by date and miner
            # find min day find max day

            # GET THE POOLS FOR FREQUENT PAYMENTS RECEIVED
            # filter dataframe to retain only tx payouts from tier1 miner list
            df_temp = df[df.from_addr.isin(tier1_miner_list)]
            df_temp = df_temp.groupBy('to_addr').agg({'from_addr': 'couunt'}).reset_index()
            threshold = threshold_tier2_pay_in * (end_date-start_date).days
            df_temp = df_temp[df_temp.to_addr >= threshold]

            tier2_miner_list = df_temp.to_addr.unique().compute()

            # save list to redis
            key_params = ['tier2_miner_list',threshold_tier2_pay_in]
            r.load(tier2_miner_list,key_params,start_date,end_date)
            del df_temp
            gc.collect()



        return tier2_miner_list
        logger.warning("tier2_miner_list:%s",tier2_miner_list)

    except Exception:
        logger.error("tier 1 miner list", exc_info=True)


