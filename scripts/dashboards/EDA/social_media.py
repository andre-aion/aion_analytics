from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import datetime_to_date
from config.dashboard import config as dashboard_config
from bokeh.models.widgets import CheckboxGroup, TextInput

from tornado.gen import coroutine
from scipy.stats import linregress

from operator import itemgetter
import pandas as pd
import dask as dd
import holoviews as hv
from holoviews import streams

from scripts.utils.myutils import tab_error_flag
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

table = 'twitter'
groupby_dict = {
    'tw_mentions': 'sum',
    'tw_positive': 'mean',
    'tw_compound': 'mean',
    'tw_neutral': 'mean',
    'tw_negative': 'mean',
    'tw_emojis_positive': 'mean',
    'tw_emojis_compound': 'mean',
    'tw_emojis_negative': 'mean',
    'tw_emojis_count': 'sum',
    'tw_replies_from_followers': 'sum',
    'tw_replies_from_following': 'sum',
    'tw_reply_hashtags': 'sum'
}

