from datetime import datetime, timedelta, date

import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, Select, TableColumn, ColumnDataSource, DataTable
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from sklearn.tree import export_graphviz
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from scipy.stats import mannwhitneyu

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.utils.dashboards.EDA.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import datetime_to_date
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
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
    'tw_mentions': 'mean',
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

