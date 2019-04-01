from datetime import datetime

from scripts.databases.pythonRedis import PythonRedis
from scripts.utils.dashboards.EDA.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from config.df_construct_config import load_columns as cols
from scripts.utils.dashboards.EDA.poolminer import is_tier1_in_memory, \
    make_tier1_list, is_tier2_in_memory, make_tier2_list
logger = mylogger(__file__)
redis = PythonRedis()


# get list keys of models dictionaries
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
def construct_from_redis(key_lst,item_type ='list',df=None,
                         table=None,df_cols=None,dedup_cols=None):
    try:
        redis = PythonRedis()
        if not key_lst:
            return None
        else:
            temp_item = [] if item_type == 'list' else df
            start_list = []
            end_list = []
            for key in key_lst:
                # create df if necessary
                if item_type == 'list':
                    item_loaded = redis.load([], '', '', key, item_type)
                    temp_item.append(item_loaded)
                    return temp_item
                else:
                    #make list of start and end dates from list

                    # get key dates
                    logger.warning('key for churned load:%s',key)

                    lst = key.split(':')
                    if lst[-1] != '':
                        req_start_date = datetime.strptime(lst[-2]+' 00:00:00', '%Y-%m-%d %H:%M:%S')
                        req_end_date = datetime.strptime(lst[-1]+' 00:00:00', '%Y-%m-%d %H:%M:%S')
                        #req_start_date = datetime.combine(req_start_date, datetime.min.time())
                        #req_end_date = datetime.combine(req_end_date, datetime.min.time())


                        start_list.append(req_start_date)
                        end_list.append(req_end_date)

            tab = Mytab('block_tx_warehouse', cols['block_tx_warehouse']['models'], [])
            if len(start_list) > 0:
                if item_type != 'list':
                    # if warehouse get minimum start date and maximum end data, and retrive from database
                    tab.key_tab = 'models'
                    req_start_date = min(start_list)
                    req_end_date = max(end_list)
                    tab.df_load(req_start_date, req_end_date)
                    logger.warning('TRACKER:%s', tab.df.tail(10))

                    return tab.df1
            else:
                return tab.df1
    except Exception:
        logger.error("construct from redis/clickhouse",exc_info=True)

# get the dictionaries, then extract the df and the lists
def extract_data_from_dict(dct_lst, df,tier=1):
    try:
        dataframe_list = []
        churned_miners_list = []
        retained_miners_list = []
        if len(dct_lst)>0:
            for dct in dct_lst:
                # load the  dictionary
                dct = redis.load([],'','',key=dct)

                # make dataframe list
                dataframe_list.append(dct['warehouse'])
                churned_miners_list = dct['churned_lst']
                retained_miners_list = dct['retained_lst']
            if len(dataframe_list)>0:
                # construct the data
                df = construct_from_redis(dataframe_list,item_type='dataframe',
                                          table='block_tx_warehouse',df=df)
        if df is not None:
            if len(df) > 0:
                # filter df by the miners
                lst = list(set(churned_miners_list + retained_miners_list))
                #logger.warning('miners list from churned:%s',lst)
                if tier == 1:
                    df = df[df.from_addr.isin(lst)]
                else:
                    df = df[df.to_addr.isin(lst)]
        return df, churned_miners_list, retained_miners_list


    except Exception:
        logger.error('extract data from dict', exc_info=True)


def get_miner_list(df,start_date,end_date,threshold_tx_paid_out,
                   threshold_blocks_mined, tier=1):
    if tier in ["1",1]:
        lst = is_tier1_in_memory(start_date,end_date,threshold_tx_paid_out,
                                 threshold_blocks_mined)
    else:
        lst = is_tier2_in_memory(start_date, end_date,
                                 start_date, end_date,
                                 threshold_tier2_pay_in=1,
                                 threshold_tx_paid_out=5,
                                 threshold_blocks_mined_per_day=0.5
                                 )

    if lst is None:
        if tier in ["1",1]:
            return make_tier1_list(df, start_date, end_date,
                                   threshold_tx_paid_out,
                                   threshold_blocks_mined)
        else:
            tier1_miners_list = get_miner_list(df,start_date,end_date,
                                               threshold_tx_paid_out,
                                               threshold_blocks_mined,
                                               tier=1)

            return make_tier2_list(df, start_date, end_date,
                                   tier1_miners_list,
                                   threshold_tier2_received=1,
                                   threshold_tx_paid_out=5,
                                   threshold_blocks_mined_per_day=0.5)
    else:
        return None