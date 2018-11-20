from os.path import dirname, join
from multiprocessing.pool import ThreadPool
from dask import compute
from fastparquet import ParquetFile, write

import pandas as pd
import dask.dataframe as dd

# os methods for manipulating paths

# Bokeh basics
from bokeh.io import curdoc
from bokeh.models.widgets import Tabs

from pdb import set_trace

# IMPORT HELPERS
#from scripts.sentiment import sentiment_dashboard
from scripts.utils.poolminer import munge_blockdetails
from scripts.utils.hashrate import calc_hashrate
from scripts.utils.myutils import convert_block_timestamp_from_string,setdatetimeindex

# GET THE DASHBOARDS
from scripts.dashboards.poolminer1 import poolminer_tab
from scripts.dashboards.hashrate import hashrate_tab

from dask.distributed import Client

# Read data into dataframes and then optimize the dataframe
columns_required = ['miner_address','block_timestamp','block_number','difficulty','block_time']
def getBlockDetails():
    """
    df_blockdetails = pd.concat((df_partial for df_partial
                                 in ParquetFile(join(dirname(__file__), 'data', 'block.parquet'))
                                .iter_row_groups(columns_required)),
                                axis=0)
    df_blockdetails['addr'] = df_blockdetails['miner_address'].str[0:10]
    df_blockdetails = convert_block_timestamp_from_string(df_blockdetails, 'block_timestamp')
    """
    df_blockdetails = dd.read_parquet(join(dirname(__file__), 'data', 'block.parquet'),
                                      engine='fastparquet',columns=columns_required)
    df_blockdetails = munge_blockdetails(df_blockdetails)
    df_blockdetails = setdatetimeindex(df_blockdetails).persist()
    return df_blockdetails

# -----------------  START DATALOAD THREADS  --------------------
threadPool = ThreadPool(processes=30)
threads = {}
df_blockdetails = threadPool.apply(getBlockDetails)
#  ----------------   TABS -----------------------------
# Create

threads['poolminer'] = threadPool.apply_async(poolminer_tab, args=(df_blockdetails[['addr','block_number']],))
'''threads['hashrate'] = threadPool.apply_async(hashrate_tab,
                                             args=(df_blockdetails[['difficulty','block_time','block_number']],))'''


#tab_hashrate = threads['hashrate'].get()
tab_poolminer = threads['poolminer'].get()

# Put all the tabs into one application
tabs = Tabs(tabs=[tab_poolminer])


# Put the tabs in the current document for display
curdoc().add_root(tabs)