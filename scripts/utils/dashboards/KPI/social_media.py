from pandas.io.json import json_normalize

from scripts.databases.pythonMongo import PythonMongo
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)


def load_df(table='etl_parameter'):
    pym = PythonMongo('aion')
    df = json_normalize(list(pym.db[table].find({})))
    logger.warning('df:%s', df)
    return df

def get_items(platform='twitter',table='etl_parameter'):
    etl_params = load_df()
    etl_params = etl_params.drop('_id', axis=1)
    etl_df = load_df(table='etl')
    etl_df = etl_df.rename(index=str, columns={'etl': 'name'})
    # join
    etl_params = etl_params.merge(etl_df, left_on='etl', right_on='_id', how='left')
    df = etl_params[etl_params.name == platform]

    items = None
    if df is not None:
        if len(df) > 0:
            items_tmp = list(set(df['label'].tolist()))
            items = []
            for item in items_tmp:
                item = item.replace(' ','_').lower()
                item = item.replace('-','_')
                items.append(item)
    return items

def get_coin_name(x,length):

    return x[:-length]


from pandas.io.json import json_normalize

from scripts.databases.pythonMongo import PythonMongo
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)

def load_df(table='etl_parameter'):
    pym = PythonMongo('aion')
    df = json_normalize(list(pym.db[table].find({})))
    logger.warning('df:%s', df)
    return df

def get_items(platform='twitter',table='etl_parameter'):
    etl_params = load_df()
    etl_params = etl_params.drop('_id', axis=1)
    etl_df = load_df(table='etl')
    etl_df = etl_df.rename(index=str, columns={'etl': 'name'})
    # join
    etl_params = etl_params.merge(etl_df, left_on='etl', right_on='_id', how='left')
    df = etl_params[etl_params.name == platform]

    items = None
    if df is not None:
        if len(df) > 0:
            items_tmp = list(set(df['label'].tolist()))
            items = []
            for item in items_tmp:
                item = item.replace(' ','_').lower()
                item = item.replace('-','_')
                items.append(item)
    return items

def set_vars(items):
    try:
        groupby_dct = {
            'external_hourly': {}
        }
        github_cols = ['watch', 'fork', 'issue', 'release', 'push']
        idvars = {
            'external_hourly': []
        }
        vars_dict = {
            'external_hourly': {
                'watch': [],
                'fork': [],
                'issue': [],
                'release': [],
                'push': [],
                'twu_tweets':[],
                'twu_mentions':[],
                'twu_positive': [],
                'twu_compound':[],
                'twu_neutral':[],
                'twu_negative':[],
                'twu_emojis_positive': [],
                'twu_emojis_compound': [],
                'twu_emojis_neutral': [],
                'twu_emojis_negative': [],
                'twu_emojis':[],
                'twu_favorites': [],
                'twu_retweets':[],
                'twu_hashtags': [],
                'twu_replies':[],

                'twr_tweets': [],
                'twr_mentions': [],
                'twr_positive': [],
                'twr_compound': [],
                'twr_neutral': [],
                'twr_negative': [],
                'twr_emojis_positive': [],
                'twr_emojis_compound': [],
                'twr_emojis_neutral': [],
                'twr_emojis_negative': [],
                'twr_emojis': [],
                'twr_favorites': [],
                'twr_retweets': [],
                'twr_hashtags': [],
                'twr_replies': [],
            }

        }
        for crypto in items:
            if crypto == 'bitcoin-cash':
                crypto = 'bitcoin_cash'
            sum_cols = ['twr_emojis','twr_favorites','twr_retweets','twr_hashtags','twr_replies','twr_tweets',
                        'twu_emojis', 'twu_favorites', 'twu_retweets', 'twu_hashtags', 'twu_replies', 'twu_tweets'] \
                       + github_cols
            for col in sum_cols:
                key = crypto + '_' + col
                vars_dict['external_hourly'][col].append(key)
                idvars['external_hourly'].append(key)
                groupby_dct['external_hourly'][key] = 'sum'

        logger.warning('groupby_dct')
        return groupby_dct, vars_dict, idvars
    except Exception:
        logger.error('set groupby dict', exc_info=True)



