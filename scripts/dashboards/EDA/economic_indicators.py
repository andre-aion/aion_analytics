import datetime

from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, WidgetBox, Select, TableColumn, ColumnDataSource, DataTable, \
    Spacer
from pandas.io.json import json_normalize

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.databases.pythonMongo import PythonMongo
from scripts.utils.interfaces.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import drop_cols
from config.dashboard import config as dashboard_config

from tornado.gen import coroutine

import holoviews as hv
from holoviews import streams

from scripts.utils.myutils import tab_error_flag
import pandas as pd

logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')


@coroutine
def eda_country_indexes_tab(panel_title):
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

            self.groupby_dict = {}

            self.div_style = """ style='width:350px; margin-left:25px;
                                    border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                                    """

            self.header_style = """ style='color:blue;text-align:center;' """
            self.countries = []
            self.country = 'Barbados'

            self.relationships_to_check = ['weak', 'moderate', 'strong']

            self.pym = PythonMongo('aion')
            self.menus = {
                'status': ['all', 'open', 'closed'],
                'gender': ['all', 'male', 'female'],
            }
            self.multiline_vars = {
                'x': '',
                'y': ''
            }
            self.timestamp_col = 'timestamp'

            # ------- DIVS setup begin
            self.page_width = 1200
            txt = """<hr/><div style="text-align:center;width:{}px;height:{}px;
                                                position:relative;background:black;margin-bottom:200px">
                                                <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                                          </div>""".format(self.page_width, 50, 'Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=self.page_width, height=20),
                'bottom': Div(text=txt, width=self.page_width, height=10),
            }
            self.section_divider = '-----------------------------------'
            self.section_headers = {
                'info': self.section_header_div(text='Country indexes')
            }


            # ----- UPDATED DIVS END

            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def load_df(self):
            try:
                df = json_normalize(list(self.pym.db[self.table].find({},{'_id':False})))
                df = df.fillna(0)
                logger.warning('LINE 96:  country indicator:%s',df.head())
                self.countries = []
                self.df = df

            except Exception:
                logger.error('load',exc_info=True)

        def get_row_column_labels(self,txt):
            x = txt.split('.')
            if x[0] not in self.countries:
                self.countries.append(x[0])
                sorted(self.countries)
            x[-1] = x[-1].replace('-','_')
            return x[0],x[-1]

        def melt_df(self):

            try:
                # logger.warning('%s',df.head(20))
                temp_dct = {
                    'country':[]
                }

                # loop through items
                counter = 0
                values_present = []

                for col in self.df.columns:
                    if col != 'timestamp':
                        # label for each coin, only run once
                        if counter == 0:
                            row,column = self.get_row_column_labels(col)
                            temp_dct['country'].append(row)
                            if column not in temp_dct.keys():
                                temp_dct[column] = []
                            try:
                                tmp = self.df[[col]]
                                val = tmp.values[0]
                            except Exception:
                                val = [0]
                            temp_dct[column].append(val[0])

                #logger.warning('LINE 140 tmp dict:%s',temp_dct)

                # find items that are not present
                # not_present = list

                counter += 1
                '''
                # logger.warning('item-length=%s-%s',key,len(temp_dct[key]))
                # convert to dataframe
                for item in temp_dct.keys():
                    # logger.warning('%s length = %s',item,len(temp_dct[item]))
                    if len(temp_dct[item]) == 0:
                        temp_dct[item] = [0] * len(temp_dct)
                '''
                self.df1 = pd.DataFrame.from_dict(temp_dct)
                # logger.warning('df after melt:%s',self.df1.head())
            except Exception:
                logger.error('melt coins', exc_info=True)

        def plot_country_rows(self,launch):
            try:
                if self.df1 is None:
                    self.melt_df()

            except Exception:
                logger.error('plot',exc_info=True)

    def update_country(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.country = thistab.country_select.value
        thistab.trigger += 1
        stream_launch_action_table.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    try:
        # SETUP
        table = 'country_indexes'
        thistab = Thistab(table, [], [])
        thistab.load_df()


        # MANAGE STREAM
        stream_launch_action_table = streams.Stream.define('Launch', launch=-1)()

        # MAKE TABLES
        # --------------------- PLOTS---------------------------------

        hv_action_table = hv.DynamicMap(thistab.plot_country_rows,
                                      streams=[stream_launch_action_table])
        action_table = renderer.get_plot(hv_action_table)

        # CREATE WIDGETS
        country_select = Select(title='Select matrix', value=thistab.load_df(),
                               options=thistab.countries)

        # handle callbacks
        country_select.on_change('value', update_country)

        # create the dashboards
        controls = WidgetBox()

        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.title_div('info', 400)],
            [Spacer(width=20, height=30)],
            [action_table.state],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('EDA projects:', exc_info=True)
        return tab_error_flag(panel_title)



