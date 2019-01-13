from scripts.storage.pythonRedis import PythonRedis
from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.utils.myutils import concat_dfs
import sys

logger = mylogger(__file__)
redis = PythonRedis()


# get list keys of churn dictionaries
def find_in_redis(item='tier1_churned_dict'):
    # get keys
    str_to_match = '*' + item + ':*'
    matches = redis.conn.scan_iter(match=str_to_match)
    lst = []
    try:
        if matches:
            for redis_key in matches:
                redis_key = str(redis_key, 'utf-8')
                logger.warning('churned_dict found:%s', redis_key)
                lst.append(redis_key)
        else:
            lst = ['no data']
        return lst

    except Exception:
        logger.error("find in redis",exc_info=True)

# get data from redis and join if necessary
def construct_from_redis(key_lst,item_type ='list',df=None,table=None,df_cols=None,dedup_cols=None):
    try:
        redis = PythonRedis()
        if not key_lst:
            return None
        else:
            temp_item = [] if item_type == 'list' else df
            for key in key_lst:
                item_loaded = redis.load([],'','',key,item_type)
                if item_type == 'list':
                    temp_item.append(item_loaded)
                elif item_type == 'dataframe':
                    temp_item = concat_dfs(temp_item,item_loaded)
        #logger.warning("CONSTRUCT FROM REDIS :%s", temp_item['approx_value'].tail(30))
        return temp_item

    except Exception:
        logger.error("construct from redis",exc_info=True)


# get the dictionaries, then extract the df and the lists
def extract_data_from_dict(dct_lst, df):
    try:
        dataframe_list = []
        churned_miners_list = []
        retained_miners_list = []
        if dct_lst:
            for dct in dct_lst:
                # load the  dictionary
                dct = redis.load([],'','',key=dct)
                # make dataframe list
                dataframe_list.append(dct['warehouse'])
                churned_miners_list = dct['churned_lst']
                retained_miners_list = dct['retained_lst']
            # construct the data
            df = construct_from_redis(dataframe_list,item_type='dataframe',
                                      table='block_tx_warehouse',df=df)

        return df, churned_miners_list, retained_miners_list

    except Exception:
        logger.error('extract data from dict', exc_info=True)

