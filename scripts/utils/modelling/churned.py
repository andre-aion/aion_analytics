from scripts.storage.pythonRedis import PythonRedis
from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.utils.myutils import concat_dfs
import sys

logger = mylogger(__file__)
redis = PythonRedis()


# get list keys of churn dictionaries
def find_in_redis(item='churned_dict'):
    # get keys
    str_to_match = '*' + item + ':*'
    matches = redis.conn.scan_iter(match=str_to_match)
    lst = ['select all']
    try:
        if matches:
            for redis_key in matches:
                redis_key_encoded = redis_key
                redis_key = str(redis_key, 'utf-8')
                logger.warning('churned_dict found:%s', redis_key)
                lst.append(redis_key)
        else:
            lst = []
        return lst

    except Exception:
        logger.error("find in redis",exc_info=True)

# get data from redis and join if necessary
def construct_from_redis(key_lst,item_type ='list',table=None,df_cols=None,dedup_cols=None):
    try:
        redis = PythonRedis()
        if not key_lst:
            return None
        else:
            temp_item = [] if item_type == 'list' else SD(table,df_cols,dedup_cols).get_df()
            for key in key_lst:
                item_loaded = redis.load([],'','',key,item_type)
                logger.warning('item loaded:%s', item_loaded)
                if item_type == 'list':
                    temp_item.append(item_loaded)
                elif item_type == 'dataframe':
                    temp_item = concat_dfs(temp_item,item_loaded)
        return temp_item

    except Exception:
        logger.error("construct from redis",exc_info=True)


# get the dictionaries, then extract the df and the lists
def extract_data_from_dict(dct_lst, cols):
    try:
        dataframe_list = []
        churned_miners_list = []
        retained_miners_list = []

        if dct_lst:
            for dct in dct_lst:
                # make dataframe list
                dataframe_list.append(dct['warehouse'])
                churned_miners_list.append(dct['churned_lst'])
                retained_miners_list.append(dct['retained_lst'])
            # construct the data
            df = construct_from_redis(dataframe_list,item_type='datafrane',
                                      table='block_tx_warehouse',df_cols=cols,dedup_cols=[])
            churned_miners_list = construct_from_redis(churned_miners_list,item_type='list')
            retained_miners_list = construct_from_redis(retained_miners_list,item_type='list')
        return df, churned_miners_list, retained_miners_list

    except Exception:
        logger.error('extract data from dict', exc_info=True)
