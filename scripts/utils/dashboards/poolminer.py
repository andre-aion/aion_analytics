from dask.dataframe.utils import make_meta

from scripts.utils.mylogger import mylogger
from scripts.storage.pythonRedis import PythonRedis
import gc
import re
from datetime import datetime
import pandas as pd

from scripts.utils.myutils import datetime_to_date

r = PythonRedis()
logger = mylogger(__file__)

def remove_char(row):
    return re.sub('\[','',row['transaction_hashes'])



def list_to_rows(df, column, sep=',', keep=False):
    """
    Split the values of a column and expand so the new DataFrame has one split
    value per row. Filters rows where the column is missing.

    Params
    ------
    df : pandas.DataFrame
        dataframe with the column to split and expand
    column : str
        the column to split and expand
    sep : str
        the string used to split the column's values
    keep : bool
        whether to retain the presplit value as it's own row

    Returns
    -------
    pandas.DataFrame
        Returns a dataframe with the same columns as `df`.
    """
    indexes = list()
    new_values = list()
    df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            # remove stray brackets
            value = re.sub('\[|\]',"",value)
            new_values.append(value)

    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values
    return new_df

# explode into new line for each list member
def explode_transaction_hashes(df):
    meta=('transaction_hashes',str)
    try:
        # remove quotes
        # explode the list
        df = list_to_rows(df,"transaction_hashes")

        return df
    except Exception:
        logger.error("explode transaction hashes", exc_info=True)



def make_warehouse(df_tx, df_block, start_date, end_date, tab='poolminer'):
    logger.warning("df_tx columns in make_poolminer_warehose:%s",df_tx.columns.tolist())
    logger.warning("df_block columns in make_poolminer_warehose:%s",df_block.columns.tolist())

    #df_tx = df_tx[['block_timestamp','transaction_hash','from_addr','to_addr','value']]
    #df_block = df_block[['miner_address','block_number','transaction_hashes']]
    df_block = df_block.drop(['block_timestamp'],axis=1)

    try:
        key_params = ['block_tx_warehouse',tab]
        meta = make_meta({
                          'block_timestamp':'M8', 'block_number': 'i8',
                          'miner_address': 'object', 'transaction_hashes': 'object'})
        df_block = df_block.map_partitions(explode_transaction_hashes)
        logger.warning('COLUMNS %s:',df_block.columns.tolist())
        df_block.reset_index()

        # join block and transaction table
        df = df_block.merge(df_tx, how='left',
                                      left_on='transaction_hashes',
                                      right_on='transaction_hash')  # do the merge\
        df = df.drop(['transaction_hashes'],axis=1)

        values = {'transaction_hash': 'unknown','value':0,
                  'from_addr':'unknown','to_addr':'unknown','block_number':0}
        df = df.fillna(value=values)
        logger.warning(("merged columns",df.columns.tolist()))
        return df
    except Exception:
        logger.error("make poolminer warehouse",exc_info=True)


def daily_percent(df,x):
    df_perc = df[(df.block_timestamp== x)]
    total = df.block_number.sum()
    return 100*x.block_number/total

def list_by_tx_paid_out(df, delta_days, threshold=5):
    lst = []
    try:

        df_temp = df.groupby('from_addr')['to_addr'].count()
        df_temp = df_temp.reset_index()
        # find daily mean
        logger.warning("tx paid out threshold:%s",threshold)
        df_temp = df_temp[df_temp.to_addr >= threshold*delta_days]
        values = {'from_addr': 'unknown'}
        df_temp = df_temp.fillna(values)
        lst1 = df_temp['from_addr'].unique().compute()
        lst = [str(x) for x in lst1]

        logger.warning("miners found by paid out: %s",len(lst))

        del df_temp
        gc.collect()
        return lst
    except Exception:
        logger.error("tx paid out", exc_info=True)


def list_from_blocks_mined_daily(df,delta_days,threshold=1):
    lst = []
    try:
        df_temp = df.groupby('miner_address')['block_number'].count().reset_index()
        total_blocks_mined_daily = 8640
        # convert percentage threshold to number
        threshold = threshold*delta_days*total_blocks_mined_daily/100
        logger.warning("blocks mined daily threshold:%s",threshold)
        df_temp = df_temp[df_temp.block_number >= threshold]
        values = {'miner_address':'unknown',
                  'block_number': 0}
        df_temp = df_temp.fillna(values)
        lst1 = df_temp['miner_address'].unique().compute()
        lst = [str(x) for x in lst1]
        logger.warning("miners found by blocks mined: %s", len(lst))

        del df_temp
        gc.collect()
        return lst
    except Exception:
        logger.error("block mined daily", exc_info=True)

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


def is_tier1_in_memory(start_date, end_date, threshold_tx_paid_out=5,
                    threshold_blocks_mined_per_day=0.5):
    try:
        # check to is if it is saved in redis
        key_params = ['tier1_miners_list', threshold_tx_paid_out,
                      threshold_blocks_mined_per_day]
        redis_key = get_key_in_redis(key_params, start_date, end_date)

        # load data from redis if saved, else compose miner list
        if redis_key is not None:
            logger.warning("TIER 1 LIST LOADED FROM REDIS")
            return r.load(key_params, start_date, end_date,
                                      key=redis_key, item_type='list')

        else:
            return None
    except Exception:
        logger.error("is_tier1_in_memory:",exc_info=True)
        return None

def make_tier1_list(df, start_date, end_date, threshold_tx_paid_out=5,
                    threshold_blocks_mined_per_day=0.5):
    try:
        #logger.warning("make tier 1 list:%s",df.head())
        # Count transactions paid out per day: group transactions by date and miner
        # find min day find max day
        key_params = ['tier1_miners_list', threshold_tx_paid_out,
                      threshold_blocks_mined_per_day]

        end_date = datetime_to_date(end_date)
        start_date = datetime_to_date(start_date)
        delta_days = (end_date - start_date).days

        if delta_days <= 0:
            delta_days = 1

        # tier 1 = percentage mined per day > threshold || transactions paid out > threshold per day#
        # make unique list of tier 1
        lst_a = list_by_tx_paid_out(df,delta_days,threshold_tx_paid_out)
        lst_b = list_from_blocks_mined_daily(df, delta_days,threshold_blocks_mined_per_day)
        # merge lists, drop duplicates
        if lst_a and lst_b:
            tier1_miners_list = list(set(lst_a + lst_b))
        else:
            if lst_a:
                tier1_miners_list = list(set(lst_a))
            elif lst_b:
                tier1_miners_list = list(set(lst_b))
            else:
                tier1_miners_list = []

        # save tier1 miner list to redis
        logger.warning("tier 1 miners list generated, before redis save:%s",len(tier1_miners_list))
        r.save(tier1_miners_list,key_params,start_date, end_date)

        del lst_a, lst_b
        gc.collect()

        return tier1_miners_list
    except Exception:
        logger.error("make tier 1 miner list", exc_info=True)


def is_tier2_in_memory(start_date, end_date,
                       threshold_tier2_pay_in=1,
                       threshold_tx_paid_out=5,
                       threshold_blocks_mined_per_day=0.5):
    try:
        # check to is if it is saved in redis
        # check to is if it is saved in redis
        key_params = ['tier2_miners_list', threshold_tier2_pay_in,
                      threshold_tx_paid_out, threshold_blocks_mined_per_day]
        redis_key = get_key_in_redis(key_params, start_date, end_date)

        # load data from redis if saved, else compose miner list
        if redis_key is not None:
            logger.warning("TIER 2 LIST LOADED FROM REDIS")

            return r.load(key_params, start_date, end_date,
                          key=redis_key, item_type='list')
        else:
            return None
    except Exception:
        logger.error("is_tier2_in_memory", exc_info=True)


def make_tier2_list(df, start_date, end_date,
                    tier1_miners_list,
                    threshold_tier2_received=1,
                    threshold_tx_paid_out=5,
                    threshold_blocks_mined_per_day=0.5
                    ):
    try:
        key_params = ['tier2_miners_list', threshold_tier2_received,
                      threshold_tx_paid_out, threshold_blocks_mined_per_day]
        # ensure both are datetimes
        if isinstance(end_date,datetime):
            end_date = end_date.date()
        if isinstance(start_date,datetime):
            start_date = start_date.date()

        delta_days = (end_date - start_date).days
        if delta_days <= 0:
            delta_days = 1

        # GET THE POOLS FOR FREQUENT PAYMENTS RECEIVED
        # filter dataframe to retain only great than
        # threshold tx pay-ins from tier1 miner list
        logger.warning("df in make tier 2 columns:%s",df.head())
        if len(tier1_miners_list)>0:
            logger.warning("length tier1 miners, in make tier 2 columns:%s",
                           len(tier1_miners_list))
            df_temp = df[df.from_addr.isin(tier1_miners_list)]
            df_temp = df_temp.groupby('to_addr')['from_addr'].count()
            df_temp = df_temp.reset_index()

            threshold = threshold_tier2_received * delta_days
            df_temp = df_temp[df_temp.from_addr >= threshold]
            #logger.warning("post_threshold filter:%s",df_temp.head())

            lst = df_temp.to_addr.unique().compute()
            tier2_miners_list = [str(x) for x in lst]

            logger.warning("tier2_miners_list:%s",len(tier2_miners_list))

            del df_temp
            gc.collect()
            if len(tier2_miners_list) > 0:
                # save list to redis
                logger.warning("tier 2 miners list generated, before redis save:%s", len(tier2_miners_list))
                r.save(tier2_miners_list, key_params, start_date, end_date)

                return tier2_miners_list
            return []
        else:
            return []

    except Exception:
        logger.error("tier 2 miner list", exc_info=True)



