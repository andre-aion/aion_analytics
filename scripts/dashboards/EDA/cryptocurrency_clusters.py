from datetime import datetime, timedelta, date

import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, Button, Select, ColumnDataSource, HoverTool, WidgetBox
from bokeh.plotting import figure, curdoc

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.databases.pythonRedis import PythonRedis
from scripts.utils.dashboards.EDA.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from config.dashboard import config as dashboard_config
from tornado.gen import coroutine

import pandas as pd
import holoviews as hv
import chartify
from holoviews import streams

from scripts.utils.myutils import tab_error_flag
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')


@coroutine
def crypto_clusters_eda_tab(cryptos,panel_title):
    global groupby_dict
    global features
    global cluster_dct
    #global source

    redis = PythonRedis()
    cluster_dct = redis.simple_load('clusters:cryptocurrencies')
    if cluster_dct is not None:
        groupby_dict = {}
        for var in cluster_dct['features']:
            groupby_dict[var] = 'sum'

        features = cluster_dct['features']
        source = {}
        for feature in features:
            source[feature] = ColumnDataSource(data=dict(
                xs=[],
                ys=[],
                labels=[],
                colors=[])
            )


    class Thistab(Mytab):
        def __init__(self, table, cols,dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols,panel_title=panel_title)
            self.table = table
            self.cols = cols
            self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
            self.df = None
            self.df1 = None
            self.df_predict = None
            self.day_diff = 1  # for normalizing for classification periods of different lengths
            self.df_grouped = ''

            self.cl = PythonClickhouse('aion')
            self.items = cryptos
            # add all the coins to the dict
            self.github_cols = ['watch','fork','issue','release','push']
            self.index_cols = ['close','high','low','market_cap','volume']

            self.trigger = 0
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                           <h1 style="color:#fff;">
                                                                           {}</h1></div>""".format('Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=1400, height=20),
                'bottom':  Div(text=txt, width=1400, height=10),
            }
            self.cluster_dct = cluster_dct
            self.groupby_dict = groupby_dict
            self.features = features
            self.crypto = 'all'

            self.div_style = """ style='width:350px; margin-left:25px;
                                    border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                                    """

            self.header_style = """ style='color:blue;text-align:center;' """

            self.significant_effect_dict = {}
            self.df1 = None
            self.section_headers = {
                'ts':self.section_header_div('Comparison of clusters across variables:---------------------',width=600)
            }
            self.timestamp_col = None
            self.colors = None


        # ----------------------  DIVS ----------------------------
        def section_header_div(self, text, html_header='h2', width=1400):
            text = '<{} style="color:#4221cc;">{}</{}>'.format(html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def information_div(self, width=400, height=300):
            txt = """
               <div {}>
               <h4 {}>How to interpret relationships </h4>
               <ul style='margin-top:-10px;'>
                   <li>
                   </li>
                   <li>
                   </li>
                   <li>
                   </li>
                   <li>
                   </li>
                    <li>
                   </li>
                    <li>
                   </li>
               </ul>
               </div>

               """.format(self.div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        # ////////////////////////// UPDATERS ///////////////////////
        def section_head_updater(self,section, txt):
            try:
                self.section_header_div[section].text = txt
            except Exception:
                logger.error('',exc_info=True)

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                    <h4 style="color:#fff;">
                    {}</h4></div>""".format(text)
            for key in self.notification_div.keys():
                self.notification_div[key].text = txt


        # /////////////////////////// LOAD CLUSTERS  //////////////////////
        def prep_data(self,df,timestamp_col):
            def label_cluster(x):
                for key,values in self.cluster_dct.items():
                    if key not in ['timestamp','variables']:
                        if x in values:
                            return key
                return x
            try:
                cols = self.features + ['crypto','timestamp']
                df = df[cols]
                # groupby and resample
                df['crypto'] = df['crypto'].map(lambda x: label_cluster(x))
                df = df.rename(columns={'crypto': 'cluster'})
                df = df.compute()
                df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
                df.set_index(timestamp_col, inplace=True)
                df = df.groupby('cluster').resample(self.resample_period).agg(self.groupby_dict)
                df.reset_index(inplace=True)
                df.set_index(timestamp_col,inplace=True)
                self.timestamp_col = timestamp_col
                self.df1 = df

            except Exception:
                logger.error('prep data', exc_info=True)

        def graph_ts(self):
            try:
                #global source
                if self.df1 is not None:
                    df = self.df1.copy()
                    clusters = df['cluster'].unique()
                    self.colors = [''] * len(clusters)
                    for idx, feature in enumerate(clusters):
                        self.colors[idx] = dashboard_config['colors'][idx]
                    if self.features is not None:
                        for idx,feature in enumerate(self.features):
                            df1 = df[['cluster',feature]]
                            # pivot into columns for cluster
                            df1 = df1.pivot(columns='cluster')
                            data = dict(
                                x = [df1.index.values] * len(clusters),
                                y = [df1[name].values for name in df1],
                                labels=clusters,
                                colors=self.colors
                            )
                            source[feature].data = data
            except Exception:
                logger.error('graph ts', exc_info=True)


        def graph_chartify(self,timestamp_col):
            try:
                # global source
                if self.df1 is not None:
                    df = self.df1.copy()
                    df = df.reset_index()

                    for feature in self.features:
                        ch = chartify.Chart(blank_labels=True, x_axis_type='datetime')
                        ch.set_title("CHARTIFY")
                        ch.plot.line(
                            # Data must be sorted by x column
                            data_frame=df.sort_values(timestamp_col),
                            x_column=timestamp_col,
                            y_column=feature,
                            color_column='cluster'
                        )
                        return ch


            except Exception:
                logger.error('graph chartify', exc_info=True)

    def update():
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.df_load(datepicker_start.value, datepicker_end.value,timestamp_col='timestamp')
        thistab.prep_data(thistab.df,'timestamp')
        thistab.graph_ts()
        thistab.notification_updater("Ready!")

    def update_resample(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.resample_period = resample_select.value
        thistab.prep_data(thistab.df,'timestamp')
        thistab.graph_ts()
        thistab.notification_updater("ready")

    try:
        table = 'crypto_daily'
        thistab = Thistab(table, [], [])

        # setup dates
        first_date_range = datetime.strptime("2018-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date'] - timedelta(days=2)
        first_date = dashboard_config['dates']['current_year_start']
        # initial function call
        thistab.df_load(first_date, last_date, timestamp_col='timestamp')
        thistab.prep_data(thistab.df,timestamp_col='timestamp')

        # MANAGE STREAMS ---------------------------------------------------------

        # CREATE WIDGETS ----------------------------------------------------------------
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)

        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        load_dates_button = Button(label="Select dates/periods, then click me!",
                                   width=20, height= 8, button_type="success")

        resample_select = Select(
            title='Select summary period',
            value= thistab.resample_period,
            options= thistab.menus['resample_periods'])

        # -------------------------------- PLOTS ---------------------------
        thistab.graph_ts()
        p = {}
        for feature in features:
            p[feature] = figure(
                x_axis_type="datetime", plot_width=1400,
                plot_height=400, title=feature)

            p[feature].multi_line(
                xs='x',
                ys='y',
                legend='labels',
                line_color='colors',
                line_width=5,
                hover_line_color='colors',
                hover_line_alpha=1.0,
                source=source[feature],

            )
            p[feature].add_tools(HoverTool(show_arrow=False, line_policy='next', tooltips=[
                ('freq', '$y'),
            ]))

        # ch = thistab.graph_chartify(timestamp_col='timestamp')
        # -------------------------------- CALLBACKS ------------------------

        load_dates_button.on_click(update)  # lags array
        resample_select.on_change('value', update_resample)

        # -----------------------------------LAYOUT ----------------------------
        # COMPOSE LAYOUT
        # put the controls in a single element
        controls_left = WidgetBox(
            datepicker_start,
            load_dates_button)

        controls_right = WidgetBox(datepicker_end)

        grid_data = [
            #[ch.figure],
            [thistab.notification_div['top']],
            [controls_left, controls_right],
            [thistab.section_headers['ts'], resample_select],

        ]
        for feature in features:
            grid_data.append([p[feature]])
            logger.warning('p:%s', p[feature])

        grid_data.append([thistab.notification_div['bottom']])

        grid = gridplot(grid_data)

        # Make a tab with the layout
        tab = Panel(child=grid, title=thistab.panel_title)
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag(thistab.panel_title)