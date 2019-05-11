from datetime import datetime, timedelta, date
from enum import Enum

import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, Select, TableColumn, ColumnDataSource, DataTable
from pandas.io.json import json_normalize

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.databases.pythonMongo import PythonMongo
from scripts.utils.dashboards.EDA.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import datetime_to_date, drop_cols
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

class RiskThreshold(Enum):
    acceptable = 1
    doubtful = 2

@coroutine
def pm_risk_assessment_tab(panel_title):
    class Thistab(Mytab):
        def __init__(self, table, cols, dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.cols = cols
            self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
            self.df = None
            self.df1 = None
            self.df_predict = None
            self.day_diff = 1  # for normalizing for classification periods of different lengths
            self.df_grouped = ''

            self.cl = PythonClickhouse('aion')

            self.trigger = 0
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                           <h1 style="color:#fff;">
                                                                           {}</h1></div>""".format('Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=1400, height=20),
                'bottom': Div(text=txt, width=1400, height=10),
            }
            self.groupby_dict = {

            }

            self.div_style = """ style='width:350px; margin-left:25px;
                                    border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                                    """

            self.header_style = """ style='color:blue;text-align:center;' """
            self.variable = 'delay_end'
            lag_section_head_txt = 'Lag relationships between {} and...'.format(self.variable)
            self.section_header_div = {
                'lag': self.title_div(lag_section_head_txt, 400),
                'distribution': self.title_div('Pre-transform distribution', 400)

            }

            self.relationships_to_check = ['weak', 'moderate', 'strong']

            self.status = 'all'
            self.gender = 'all'
            self.type = 'all'
            self.ratings = {
                'severity':
                    {
                        'Insignificant' : 1,
                        'Minor' : 2,
                        'Moderate' : 3,
                        'Critical': 4,
                        'Catastrophic' :5
                    },
                'likelihood':
                    {
                        'Unlikely':1,
                        'Seldom':2,
                        'Occaisional':3,
                        'Likely':4,
                        'Definite':5
                    }
            }

            self.variables = {
                'severity': list(self.ratings['severity'].keys()),
                'likelihood': list(self.ratings['likelihood'].keys()),
            }
            self.pym = PythonMongo('aion')
            self.menus = {
                'status': ['all', 'open', 'closed'],
                'gender': ['all', 'male', 'female'],
            }
            self.multiline_vars = {
                'x': 'manager_gender',
                'y': 'remuneration'
            }
            self.timestamp_col = 'analysis_date'

            self.risks = []
            self.risk = ''
            self.matrices = []
            self.matrix = ''
            self.risk_select = Select(title='Select risk',value=self.risk,options=self.risks)


        def load_df(self):
            try:
                risk_matrix = json_normalize(list(self.pym.db['risk_matrix'].find()))
                risk_matrix = drop_cols(risk_matrix,['desc'])
                risk = json_normalize(list(self.pym.db['risk'].find()))
                risk = risk.rename(columns={'matrix':'matrix_id'})
                analysis = json_normalize(list(self.pym.db['risk_analysis'].find()))
                analysis = drop_cols(analysis,['_id'])
                analysis = analysis.rename(columns={'risk':'risk_id'})

                # merges
                risk = risk.merge(analysis,how='inner',left_on='_id',right_on='risk_id')
                risk = drop_cols(risk,['_id','likelihood_comment','severity_comment','desc','risk_id'])
                risk = risk_matrix.merge(risk,how='inner',left_on='_id',right_on='matrix_id')
                df = drop_cols(risk,['_id','matrix_id','analyst'])
                df = df.rename(columns={'name':'matrix'})
                dfs = {}
                for component in ['severity','likelihood']:
                    table = 'risk_'+ component
                    dfs[component] = json_normalize(list(self.pym.db[table].find()))

                    dfs[component] = drop_cols(dfs[component],['desc','level'])
                    df = df.merge(dfs[component],how='left',left_on=component,right_on='_id')
                    df = drop_cols(df,['_id','project',component])
                    df = df.rename(columns={'value':component})
                    df[component] = df[component].fillna(0)
                df['composite'] = df.severity * df.likelihood

                # set selection variables
                logger.warning('LINE 154 df:%s',df)
                self.df = df
                self.matrices = list(df['matrix'].unique())
                self.matrix = self.matrices[0]
                self.set_risks(df,matrix=self.matrix)

            except Exception:
                logger.error('load df', exc_info=True)

        def set_risks(self,df,matrix):
            try:

                df = df[df.matrix == matrix]
                self.risks = list(df['risk'].unique())
                self.risk = self.risks[0]
                self.risk_select.options = self.risks

                self.df1 = df
            except Exception:
                logger.error('prep data', exc_info=True)


        # //////////////  DIVS   //////////////////

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        # ////////////// GRAPHS ////////////////////

        def action_table(self, launch):
            try:
                def label_action(x):
                    if x < RiskThreshold.acceptable:
                        return 'Proceed (risk is acceptable)'
                    elif x < RiskThreshold.doubtful:
                        return 'Proceed, if no other options are available'
                    else:
                        return 'Do no proceed (Risk unacceptable)'

                df = self.df
                df = df.groupby(['matrix', 'risk']).agg({'likelihood': 'mean', 'severity': 'mean'})

                df['action'] = df['composite'].map(label_action)

                self.df.hvplot.table(columns=['matrix', 'risk', 'severity', 'action'])
            except Exception:
                logger.error('action table', exc_info=True)

        def risk_matrix(self,launch):
            try:
                # filter
                df = self.df1
                #df = self.df.groupby(['matrix', 'risk']).agg({'likelihood': 'mean', 'severity': 'mean'})
                logger.warning('LINE 208 df:%s',list(df.columns))
                df = df[df['risk'] == self.risk]
                severity = int(df['severity'].mean())
                likelihood =  int(df['likelihood'].mean())

                # make the matrix
                dct = {
                    'severity':list(self.ratings['severity'].keys()),
                }
                cols = list(self.ratings['likelihood'].keys())
                for idx_r,col in enumerate(list(self.ratings['severity'].keys())):
                    dct[col] = []
                    for idx_c,val in enumerate(cols):
                        if idx_r == self.ratings['severity'][severity] and \
                                idx_c == self.ratings['likelihood'][likelihood]:
                            dct[col].append('bingo')
                        else:
                            dct[col].append(str(idx_c * idx_r))
                    logger.warning('%s - length=%s',col,len(dct[col]))


                df_matrix = pd.DataFrame.from_dict(dct)
                cols += ['severity']
                return df_matrix.hvplot.table(columns=cols,width=800)
            except Exception:
                logger.error('risk matrix', exc_info=True)

    def update_matrix(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.matrix = matrix_select.value
        thistab.set_risks(thistab.df,matrix=thistab.matrix)
        thistab.trigger += 1
        stream_launch_action_table.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_risk(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.risk = thistab.risk_select.value
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    try:
        # SETUP
        table = 'project_composite'
        thistab = Thistab(table, [], [])
        thistab.load_df()

        # MANAGE STREAM
        stream_launch_action_table = streams.Stream.define('Launch', launch=-1)()
        stream_launch_matrix = streams.Stream.define('Launch', launch=-1)()


        hv_matrix = hv.DynamicMap(thistab.risk_matrix,
                                       streams=[stream_launch_matrix])
        matrix = renderer.get_plot(hv_matrix)

        hv_action_table = hv.DynamicMap(thistab.action_table,
                                      streams=[stream_launch_action_table])
        action_table = renderer.get_plot(hv_action_table)



        # CREATE WIDGETS
        matrix_select = Select(title='Select matrix', value=thistab.matrix,
                               options=thistab.matrices)

        # handle callbacks
        matrix_select.on_change('value', update_matrix)
        thistab.risk_select.on_change('value', update_risk)

        # create the dashboards

        grid = gridplot([
            [thistab.notification_div['top']],
            [matrix_select, thistab.risk_select],
            [thistab.title_div('Determine action', 400)],
            [action_table],
            [thistab.title_div('Matrix', 400)],
            [matrix],
            [thistab.notification_div['bottom']]

        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('EDA projects:', exc_info=True)
        return tab_error_flag(panel_title)