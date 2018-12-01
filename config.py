# GLOBAL VARIABLES
from scripts.streaming.streamingBlock import Block

from streamz import Stream
from streamz.dataframe import DataFrame
import pandas as pd
import dask as dd
from tornado import gen
from tornado.locks import Lock
from copy import copy
import gc

block = Block()
stream = Stream()

# instantiate
#sdf = DataFrame(example=block.get_df(), stream=stream)

temp = {}
temp['block_number'] = list(range(1,100000))
temp['difficulty'] = [a*a for a in temp['block_number']]
df_temp = pd.DataFrame.from_dict(temp)
df_test = dd.dataframe.from_pandas(df_temp, npartitions=15)

lock = Lock()

def df(max_load_value=26):
    df =block.get_df()
    while len(df) == 0 or len(df) <= max_load_value:
        df = block.get_df()
    return df

