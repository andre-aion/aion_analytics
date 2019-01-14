from datetime import datetime

from scripts.storage.pythonRedis import PythonRedis
from scripts.utils.dashboard.mytab import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import concat_dfs
from data.config import load_columns as cols
from scripts.utils.dashboard.poolminer import is_tier1_in_memory, \
    make_tier1_list
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
                # create df if necessary
                logger.warning('key for churned load:%s',key)
                item_loaded = redis.load([],'','',key,item_type)
                logger.warning('item loaded in churned load:%s',item_loaded)
                if item_type == 'list':
                    temp_item.append(item_loaded)
                else:
                    if item_loaded is None:  # create database if it is not loaded
                        # get key dates
                        lst = key.split(':')
                        req_start_date = datetime.strptime(lst[-2], '%Y-%m-%d')
                        req_end_date = datetime.strptime(lst[-1], '%Y-%m-%d')
                        tab = Mytab('block_tx_warehouse', cols['block_tx_warehouse']['churn'], [])
                        tab.key_tab = 'churn'
                        tab.df_load(req_start_date, req_end_date)
                        item_loaded = tab.df1
                    temp_item = concat_dfs(temp_item, item_loaded)
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
        # filter df by the miners
        lst = list(set(churned_miners_list + retained_miners_list))
        #logger.warning('miners list from churned:%s',lst)
        df = df[df.from_addr.isin(lst)]
        return df, churned_miners_list, retained_miners_list

    except Exception:
        logger.error('extract data from dict', exc_info=True)


def get_miner_list(df,start_date,end_date,threshold_tx_paid_out,
             threshold_blocks_mined, tier=1):
    if tier in ["1",1]:
        lst = is_tier1_in_memory(start_date,end_date,threshold_tx_paid_out,
                             threshold_blocks_mined)
    if lst is None:
        if tier in ["1",1]:
            return make_tier1_list(df, start_date, end_date,
                                   threshold_tx_paid_out,
                                   threshold_blocks_mined)
    else:
        return None