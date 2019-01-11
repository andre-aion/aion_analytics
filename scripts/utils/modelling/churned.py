from scripts.storage.pythonRedis import PythonRedis
from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.utils.myutils import concat_dfs

logger = mylogger(__file__)
# get list keys of churn dictionaries
def find_in_redis(item='churned_dict'):
    redis = PythonRedis()
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
        logger.error("construct from reid",exc_info=True)

