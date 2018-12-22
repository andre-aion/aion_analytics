from pandas.api.types import is_string_dtype
from pdb import set_trace
import dask as dd
import gc
import config
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)

def str_to_hex(x):
    logger.warning("the str to convert:%s", x)
    if isinstance(x,int):
        return x
    elif isinstance(x,str):
        return int(x, base=16)
    else:
        return 0

def calc_hashrate(df,blockcount):
    #logger.warning('columns in calc hashrate:%s',df.columns.values)
    df['rolling_mean'] = df.block_time.rolling(blockcount).mean().compute()
    df = df.dropna()
    df['hashrate'] = df.difficulty / df.rolling_mean
    return df

