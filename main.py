from os.path import dirname, join
from multiprocessing.pool import ThreadPool

import pandas as pd
# os methods for manipulating paths

# Bokeh basics
from bokeh.io import curdoc
from bokeh.models.widgets import Tabs

# IMPORT HELPERS
#from scripts.sentiment import sentiment_dashboard
from scripts.utils.poolminer import munge_blockdetails

# GET THE DASHBOARDS
from scripts.dashboards.poolminer import poolminer_tab

# Read data into dataframes and then optimize the dataframe
def getBlockDetails():
    df_blockdetails = pd.read_csv(join(dirname(__file__), 'data', 'blockdetails.csv'))
    df_poolinfo = pd.read_csv(join(dirname(__file__), 'data', 'poolinfo.csv'))
    df_blockdetails = munge_blockdetails(df_blockdetails, df_poolinfo)
    return df_blockdetails

# -----------------  START DATALOAD THREADS  --------------------
threadPool = ThreadPool(processes=30)
threads = {}
threads['blockdetails'] = threadPool.apply_async(getBlockDetails)



#  ----------------   TABS -----------------------------
# Create the tabs
tab1 = poolminer_tab(threads['blockdetails'].get())

# Put all the tabs into one application
tabs = Tabs(tabs=[tab1])



# Put the tabs in the current document for display
curdoc().add_root(tabs)