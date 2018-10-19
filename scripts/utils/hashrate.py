from pandas.api.types import is_string_dtype
from pdb import set_trace
import dask as dd
import gc

def str_to_hex(x):
    return int(x, base=16)


def calc_hashrate(df,blockcount):
    df['rolling_mean'] = df.block_time.rolling(blockcount).mean()
    df = df.dropna()
    #if string conver to int
    if df.difficulty.dtype == 'O':
        df['difficulty'] = df.difficulty.map(lambda x: int(x,base=16), meta=('x',int))
    df['hashrate'] = df.difficulty / df.rolling_mean
    df = df.drop(['rolling_mean','difficulty'],axis=1)
    gc.collect()
    return df