import random

from bokeh.plotting import figure
from holoviews import streams, dim

from scripts.databases.pythonMongo import PythonMongo
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock
from scripts.utils.interfaces.KPI_interface import KPI
from config.dashboard import config as dashboard_config

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel, Button, Spacer, HoverTool, Range1d, ColumnDataSource, ResetTool, BoxZoomTool, PanTool, \
    ToolbarBox, Toolbar, SaveTool, WheelZoomTool
import gc
from bokeh.models.widgets import Div, \
    DatePicker, Select

from datetime import datetime, timedelta, date

import holoviews as hv
from holoviews import opts
from tornado.gen import coroutine
import numpy as np
import pandas as pd

from static.css.KPI_interface import KPI_card_css

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')


@coroutine
def KPI_projects_tab(panel_title, DAYS_TO_LOAD=90):
    timeline_source = ColumnDataSource(data=dict(
        Item=[],
        Start=[],
        End=[],
        Color=[],
        start=[],
        end=[],
        ID=[],
        ID1=[]
    ))
    class Thistab(KPI):
        def __init__(self, table, cols=[]):
            KPI.__init__(self, table, name='project', cols=cols)
            self.table = table
            self.df = None
            self.df_pop = None

            self.checkboxgroup = {}
            self.period_to_date_cards = {

            }
            self.ptd_startdate = datetime(datetime.today().year, 1, 1, 0, 0, 0)

            self.timestamp_col = 'project_startdate_actual'
            self.pym = PythonMongo('aion')
            self.groupby_dict = {
                'project': 'sum',
                'project_duration': 'sum',
                'project_start_delay': 'mean',
                'project_end_delay': ' mean',

                'milestone': 'sum',
                'milestone_duration': 'sum',
                'milestone_start_delay': 'mean',
                'milestone_end_delay': ' mean',

                'task': 'sum',
                'task_duration': 'sum',
                'task_start_delay': 'mean',
                'task_end_delay': ' mean',
            }
            
            self.menus = {
                'status' : ['all','open','closed'],
                'type':['all','research','reconciliation','audit','innovation',
                        'construction','manufacturing','conference'],
                'gender':['all','male','female'],
                'variables':list(self.groupby_dict.keys()),
                'history_periods': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
            }

            self.status = 'all'
            self.pm_gender = 'all'
            self.m_gender = 'all'
            self.t_gender = 'all'
            self.type = 'all'
            self.variables = sorted(list(self.groupby_dict.keys()))
            self.variable = self.variables[0]
            self.groupby_var = 'project'


            self.chord_data = {
                'rename': {

                    'project_owner':'source',
                    'milestone_owner':'target',
                    'remuneration':'value'
                },
                'percentile_threshold':.75,

            }

            self.percentile_threshold = 10

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
                'cards': self.section_header_div(text='Period to date:{}'.format(self.section_divider),
                                                 width=1000, html_header='h2', margin_top=50,margin_bottom=5),
                'pop': self.section_header_div(text='Period over period:{}'.format(self.section_divider),
                                               width=600, html_header='h2', margin_top=5,margin_bottom=-155),
                'chord': self.section_header_div(text='Relationships:{}'.format(self.section_divider),
                                                 width=600, html_header='h2', margin_top=5,margin_bottom=-155),
                'timeline' :self.section_header_div(text='Project timeline:{}'.format(self.section_divider),
                                                 width=600, html_header='h2', margin_top=5,margin_bottom=-155),
            }
            self.KPI_card_div = self.initialize_cards(self.page_width, height=350)
            start = datetime(2014,1,1,0,0,0)
            end = datetime(2019, 5, 15, 0, 0, 0)
            self.tools = [BoxZoomTool(),ResetTool(),PanTool(),SaveTool(),WheelZoomTool()]
            self.timeline_vars = {
                'projects' : '',
                'project':'',
                'types' :['all','milestone','task','project'],
                'type':'all',
                'DF':None,
                'G':figure(
                    title=None, x_axis_type='datetime', width=1200, height=900,
                    y_range=[], x_range=Range1d(start, end), toolbar_location=None),
                'toolbar_box': ToolbarBox()
            }


            # ----- UPDATED DIVS END

        # ----------------------  DIVS ----------------------------
        def section_header_div(self, text, html_header='h2', width=600, margin_top=150,margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>"""\
                .format(margin_top,margin_bottom,html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def information_div(self, width=400, height=300):
            txt = """
            <div {}>
                <h4 {}>How to interpret sentiment score</h4>
                <ul style='margin-top:-10px;'>
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



        def initialize_cards(self,width,height=250):
            try:
                txt = ''
                for period in ['year','quarter','month','week']:
                    design = random.choice(list(KPI_card_css.keys()))
                    txt += self.card(title='',data='',card_design=design)

                text = """<div style="margin-top:100px;display:flex; flex-direction:row;">
                {}
                </div>""".format(txt)
                div = Div(text=text, width=width, height=height)
                return div
            except Exception:
                logger.error('initialize cards', exc_info=True)


        def load_df(self,req_startdate, req_enddate, table, cols, timestamp_col):
            try:
                # get min and max of loaded df
                if self.df is not None:
                    loaded_min = self.df[timestamp_col].min()
                    loaded_max = self.df[timestamp_col].max()

                    if loaded_min <= req_startdate and loaded_max >= req_enddate:
                        df = self.df[(self.df[timestamp_col] >= req_startdate) &
                                     (self.df[timestamp_col] <= req_enddate)]
                        return df
                return self.pym.load_df(req_startdate, req_enddate, table=table,
                                      cols=cols, timestamp_col=timestamp_col)

            except Exception:
                logger.error('load_df', exc_info=True)

        def filter_df(self, df1):
            if self.status != 'all':
                df1 = df1[df1.status == self.status]
            if self.pm_gender != 'all':
                df1 = df1[df1.project_owner_gender == self.pm_gender]
            if self.m_gender != 'all':
                df1 = df1[df1.milestone_owner_gender == self.m_gender]
            if self.t_gender != 'all':
                df1 = df1[df1.task_owner_gender == self.t_gender]

            if self.type != 'all':
                df1 = df1[df1.type == self.type]
            return df1

        def period_to_date(self, df, timestamp=None, timestamp_filter_col=None, cols=[], period='week'):
            try:
                if timestamp is None:
                    timestamp = datetime.now()
                    timestamp = datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, 0, 0)

                start = self.first_date_in_period(timestamp, period)
                # filter

                df[timestamp_filter_col] = pd.to_datetime(df[timestamp_filter_col], format=self.DATEFORMAT_PTD)
                #logger.warning('df:%s', df[self.timestamp_col])

                df = df[(df[timestamp_filter_col] >= start) & (df[timestamp_filter_col] <= timestamp)]
                if len(cols) > 0:
                    df = df[cols]
                return df
            except Exception:
                logger.error('period to date', exc_info=True)

        def period_over_period(self, df, start_date, end_date, period,
                               history_periods=2, timestamp_col='timestamp_of_first_event'):
            try:
                # filter cols if necessary
                string = '0 {}(s) prev(current)'.format(period)

                # filter out the dates greater than today
                df_current = df.copy()
                df_current = self.filter_df(df_current)
                df_current['period'] = string
                # label the days being compared with the same label
                df_current = self.label_dates_pop(df_current, period, timestamp_col)
                cols = [self.variable,'period','dayset']
                if 'project' in self.variable:
                    if self.variable != 'project':
                        df_current = df_current[[self.variable,'period','dayset','project']]
                elif 'milestone' in self.variable:
                    if self.variable != 'milestone':
                        df_current = df_current[[self.variable,'period','dayset','milestone','project']]
                elif 'task' in self.variable:
                    if self.variable != 'task':
                        df_current = df_current[[self.variable,'period','dayset','task','milestone','project']]

                # zero out time information
                start = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
                end = datetime(end_date.year, end_date.month, end_date.day, 0, 0, 0)

                #cols = list(df.columns)
                counter = 1
                if isinstance(history_periods, str):
                    history_periods = int(history_periods)
                # make dataframes for request no. of periods
                start, end = self.shift_period_range(period, start, end)
                while counter < history_periods and start >= self.initial_date:
                    # load data
                    df_temp = self.load_df(start, end, table=self.table, cols=[], timestamp_col=timestamp_col)
                    df_temp = self.filter_df(df_temp)
                    df_temp[timestamp_col] = pd.to_datetime(df_temp[timestamp_col])
                    if df_temp is not None:
                        if len(df_temp) > 1:
                            string = '{} {}(s) prev'.format(counter, period)
                            # label period
                            df_temp = df_temp.assign(period=string)
                            # relabel days to get matching day of week,doy, dom, for different periods
                            df_temp = self.label_dates_pop(df_temp, period, timestamp_col)
                            df_temp = df_temp[cols]
                            # logger.warning('df temp loaded for %s previous: %s',counter,len(df_temp))

                            df_current = pd.concat([df_current, df_temp])
                            del df_temp
                            gc.collect()
                    # shift the loading window
                    counter += 1
                    start, end = self.shift_period_range(period, start, end)
                return df_current
            except Exception:
                logger.error('period over period', exc_info=True)

            # label dates for period over period (pop)

        def label_dates_pop(self, df, period, timestamp_col):
            #df[timestamp_col] = pd.to_datetime(df[timestamp_col])
            def label_qtr_pop(y):
                try:
                    curr_quarter = int((y.month - 1) / 3 + 1)
                    start = datetime(y.year, 3 * curr_quarter - 2, 1)
                    return abs((start - y).days)
                except Exception:
                    logger.error('df label quarter', exc_info=True)

            try:
                if period == 'week':
                    df['dayset'] = df[timestamp_col].dt.dayofweek
                elif period == 'month':
                    df['dayset'] = df[timestamp_col].dt.day
                elif period == 'year':
                    #logger.warning('LINE 218:%s', df.head(5))
                    df['dayset'] = df[timestamp_col].dt.dayofyear
                elif period == 'quarter':
                    df['dayset'] = df[timestamp_col].apply(lambda x: label_qtr_pop(x))

                return df
            except Exception:
                logger.error('label data ', exc_info=True)


        def get_groupby_pop_df(self,df,variable,groupby_cols):
            try:
                if variable in ['project']:
                    df = df.groupby(groupby_cols).agg({variable:'count'})
                    df = df.reset_index()
                    #logger.warning('LINE 286 df:%s',df)
                elif variable in ['milestone']:
                    df = df.groupby(groupby_cols).agg({variable: 'count'})
                    df = df.reset_index()
                    #logger.warning('LINE 291 df:%s', df)
                elif variable in ['task']:
                    df = df.groupby(groupby_cols).agg({variable: 'count'})
                    df = df.reset_index()
                elif variable in ['remuneration']:
                    df = df.groupby(groupby_cols).agg({variable: 'sum'})
                    df = df.reset_index()
                else:
                    #logger.warning('LINE 259:df:%s',df.head())
                    df = df.groupby(groupby_cols).agg({variable: 'mean'})
                    df = df.reset_index()

                # clean up
                if self.groupby_var in df.columns and self.variable != self.groupby_var:
                    df = df.drop([self.groupby_var],axis=1)

                return df
            except Exception:
                logger.error('get groupby card data', exc_info=True)

        def get_groupby_card_data(self,df,variable):
            try:
                if variable in ['project']:
                    data = len(df[variable].unique())
                    data = "{} {}s".format(data, variable)
                elif variable in ['milestone']:
                    df = df.groupby(['project']).agg({variable:'nunique'})
                    data = df[variable].sum()
                    data = "{} {}s".format(data, variable)
                elif variable in ['task']:
                    df = df.groupby(['project','milestone']).agg({variable:'count'})
                    data = df[variable].sum()
                    data = "{} {}s".format(data, variable)
                elif variable in ['project_duration'] or 'delay' in variable:
                    df = df.groupby([self.groupby_var]).agg({variable: 'mean'})
                    df = df.reset_index()
                    data = "{} days".format(round(df[variable].sum(), 2))
                elif variable in ['milestone_duration']:
                    df = df.groupby([self.groupby_var,'project']).agg({variable: 'mean'})
                    df = df.reset_index()
                    data = "{} days".format(round(df[variable].sum(), 2))
                elif variable in ['task_duration','task_start_delay', 'task_start_end']:
                    df = df.groupby([self.groupby_var,'project','milestone']).agg({variable: 'mean'})
                    df = df.reset_index()
                    data = "{} hours".format(round(df[variable].sum(), 2))
                elif variable in ['remuneration']:
                    data = df[variable].sum()
                    data = "${:,.2f}".format(data)

                return data
            except Exception:
                logger.error('get groupby card data', exc_info=True)



        # -------------------- GRAPHS -------------------------------------------
        def graph_periods_to_date(self, df2, timestamp_filter_col,variable):
            df1 = df2.copy()
            #self.section_header_updater(section='cards',label=variable,margin_top=159,html_header='h2')
            try:
                df1 = self.filter_df(df1)
                dct = {}
                for idx, period in enumerate(['week', 'month', 'quarter', 'year']):
                    df = self.period_to_date(df1, timestamp=dashboard_config['dates']['last_date'],
                                             timestamp_filter_col=timestamp_filter_col, period=period)

                    df = df.drop_duplicates(keep='first')

                    # groupby to eliminate repetition
                    data = self.get_groupby_card_data(df,variable)

                    del df
                    gc.collect()
                    dct[period]=data
                    #title = "{} to date".format(period)

                    #p = self.card(title=title, data=data, card_design=random.choice(list(self.KPI_card_css.keys())))
                    #self.period_to_date_cards[period].text = p.text
                self.update_cards(dct)

            except Exception:
                logger.error('graph periods to date', exc_info=True)

        def graph_period_over_period(self, period):
            try:

                periods = [period]
                start_date = self.pop_start_date
                end_date = self.pop_end_date
                if isinstance(start_date, date):
                    start_date = datetime.combine(start_date, datetime.min.time())
                if isinstance(end_date, date):
                    end_date = datetime.combine(end_date, datetime.min.time())
                today = datetime.combine(datetime.today().date(), datetime.min.time())

                df = self.df_pop.copy()
                df = self.filter_df(df)
                #logger.warning('LINE 363 -df:%s',df.head())

                cols = [self.variable, self.timestamp_col]
                if self.variable != 'project':
                    cols.append('project')

                if abs(start_date - end_date).days > 7:
                    if 'week' in periods:
                        periods.remove('week')
                if abs(start_date - end_date).days > 31:
                    if 'month' in periods:
                        periods.remove('month')
                if abs(start_date - end_date).days > 90:
                    if 'quarter' in periods:
                        periods.remove('quarter')
                for idx, period in enumerate(periods):
                    df_period = self.period_over_period(df, start_date=start_date, end_date=end_date,
                                                        period=period, history_periods=self.pop_history_periods,
                                                        timestamp_col=self.timestamp_col)

                    groupby_cols = ['dayset', 'period']
                    df_period = self.get_groupby_pop_df(df_period,variable=self.variable,groupby_cols=groupby_cols)

                    prestack_cols = list(df_period.columns)
                    df_period = self.split_period_into_columns(df_period, col_to_split='period',
                                                               value_to_copy=self.variable)

                    # short term fix: filter out the unnecessary first day added by a corrupt quarter functionality
                    if period == 'quarter':
                        min_day = df_period['dayset'].min()
                        df_period = df_period[df_period['dayset'] > min_day]

                    poststack_cols = list(df_period.columns)

                    title = "{} over {}".format(period, period)
                    plotcols = list(np.setdiff1d(poststack_cols, prestack_cols))
                    if self.variable in ['task_start_delay','task_end_delay','task_duration']:
                        ylabel = 'hours'
                    elif self.variable in [
                        'project_duration','milestone_duration',
                        'project_start_delay','project_end_delay',
                        'milestone_start_delay','milestone_end_delay']:
                        ylabel = 'days'
                    elif self.variable in ['project','task','milestone']:
                        ylabel = '#'
                    elif self.variable == 'remuneration':
                        ylabel = '$'

                    if idx == 0:
                        p = df_period.hvplot.bar('dayset', plotcols, rot=45, title=title,
                                                 stacked=False, width=1200, height=400, value_label=ylabel)
                    else:
                        p += df_period.hvplot.bar('dayset', plotcols, rot=45, title=title,
                                                  stacked=False, width=1200, height=400, value_label=ylabel)
                return p

            except Exception:
                logger.error('period over period to date', exc_info=True)

        def chord_diagram(self, launch):
            try:
                def normalize_value(x,total):
                    x = int((x/total)*1000)
                    if x <= 0:
                        return 1
                    return x


                df = self.df.copy()

                # --------------  nodes
                data = {}
                data['nodes'] = []
                source_list = df['milestone_owner'].tolist()
                names = list(set(source_list))

                person_type_dict = dict(zip(df.milestone_owner,df.type))
                type_dict = {}
                types = list(set(df['type'].tolist()))
                name_dict = {}
                for idx,name in enumerate(names):
                    name_dict[name] = idx

                for idx,name in enumerate(names):
                    type_tmp = person_type_dict[name]
                    index = name_dict[name]
                    data['nodes'].append({'OwnerID':index,'index':idx,'Type':type_tmp})

                nodes = hv.Dataset(pd.DataFrame(data['nodes']),'index')

                # --------- make the links

                data['links'] = []

                for idx,row in df.iterrows():
                    src = name_dict[row['project_owner']]
                    tgt = name_dict[row['milestone_owner']]
                    val = row['remuneration']
                    data['links'].append({'source':src,'target':tgt,'value':val})

                links = pd.DataFrame(data['links'])
                # get the individual links
                links = links.groupby(['source','target'])['value'].sum()
                links = links.reset_index()
                total = links['value'].sum()
                links['value'] = links['value'].apply(lambda x: normalize_value(x,total))

                # filter for top percentile
                quantile_val = links['value'].quantile(self.chord_data['percentile_threshold'])
                links = links[links['value'] >= quantile_val]
                #logger.warning('after quantile filter:%s',len(links))

                chord_ = hv.Chord((links, nodes),['source','target'],['value'])
                chord_.opts(opts.Chord(cmap='Category20',edge_cmap='Category20',edge_color=dim('source').str(),
                                       labels='Type',node_color=dim('index').str(),width=1000,height=1000))

                return chord_

            except Exception:
                logger.error('chord diagram', exc_info=True)

        def timeline(self,project,type='milestone'):
            try:
                DF = self.df.copy()
                if type != project:
                    DF = DF[DF['project']==project]

                if type == 'all': 
                    rename_dct = {
                        'milestone_enddate_proposed':'milestone_enddate',
                        'milestone_startdate_proposed':'milestone_startdate',
                        'task_enddate_proposed': 'task_enddate',
                        'task_startdate_proposed': 'task_startdate',
                                  }
                    DF = DF.rename(index=str, columns=rename_dct)

                    DF = DF.groupby(['milestone','task']).agg({
                        'milestone_startdate': 'min', 'milestone_enddate': 'max',
                        'task_startdate': 'min', 'task_enddate': 'max',
                    })
                    DF = DF.reset_index()

                    # melt to get milestone and task into one column
                    df = pd.melt(DF,value_vars=['milestone','task'],
                                      id_vars=['milestone_startdate','milestone_enddate',
                                               'task_startdate','task_enddate'],
                                      value_name='Item',var_name='type')

                    df = df.groupby(['Item','type']).agg({
                        'milestone_startdate':'min',
                        'milestone_enddate':'max',
                        'task_startdate': 'min',
                        'task_enddate': 'max'
                    }).reset_index()
                    df = pd.melt(df, id_vars=['Item', 'type','milestone_startdate','task_startdate'],
                                 value_vars=['milestone_enddate','task_enddate'],
                                 value_name='End',var_name='enddate_type'
                                 )
                    # filter out where tasks label dates and vice versa
                    df1 = df[(df['type'] == 'task') & (df['enddate_type'] =='task_enddate')]
                    df = df[(df['type'] == 'milestone') & (df['enddate_type'] == 'milestone_enddate')]
                    df = pd.concat([df1,df])
                    df = df.drop('enddate_type',axis=1)

                    # do startdate
                    df = pd.melt(df, id_vars=['Item', 'type', 'End'],
                                 value_vars=['milestone_startdate', 'task_startdate'],
                                 value_name='Start', var_name='startdate_type'
                                 )
                    # filter out where tasks label dates and vice versa
                    df1 = df[(df['type'] == 'task') & (df['startdate_type'] == 'task_startdate')]
                    df = df[(df['type'] == 'milestone') & (df['startdate_type'] == 'milestone_startdate')]
                    df = pd.concat([df1, df])
                    df = df.drop('startdate_type', axis=1)
                    # label colors
                    df['Color'] = df['type'].apply(lambda x: 'black' if x == 'milestone' else 'green')
                    # organize by milestone and tasks belonging to milestone
                    df = df.sort_values(by=['Start']).reset_index()
                    df = df.drop('index',axis=1)
                    #logger.warning('LINE 605 - df:%s',df.head(50))
                    DF = df
                    print('##################################################################################')
                else:
                    start_str = type + '_startdate_proposed'
                    end_str = type + '_enddate_proposed'
                    # group milestone
                    rename_dct = {
                        start_str : 'Start',
                        end_str:'End',
                        type : 'Item'
                    }
                    DF = DF.rename(index=str, columns=rename_dct)
                    DF = DF[['Item', 'Start', 'End']]
                    DF = DF.groupby(['Item']).agg({'Start':'min','End':'max'})
                    DF = DF.reset_index()

                    color_list = []
                    for item in DF.Item.tolist():
                        color_list.append(random.choice(dashboard_config['colors']))
                    DF['Color'] =  np.array(color_list)

                DF['start'] = DF['Start'].dt.strftime('%Y-%m-%d')
                DF['end'] = DF['End'].dt.strftime('%Y-%m-%d')
                DF['ID'] = DF.index + 0.6
                DF['ID1'] = DF.index + 1.4

                logger.warning('LINE 648 %s',DF)
                self.timeline_vars['DF'] = DF
                # update source
                data = dict(
                    Item = DF.Item.tolist(),
                    Start = DF.Start.tolist(),
                    End = DF.End.tolist(),
                    Color= DF.Color.tolist(),
                    start=DF.start.tolist(),
                    end=DF.end.tolist(),
                    ID = DF.ID.tolist(),
                    ID1 = DF.ID1.tolist()
                )
                # <-- This is the trick, make the x_rage empty first, before assigning new value

                self.timeline_vars['G'].y_range.factors = []
                self.timeline_vars['G'].y_range.factors = DF.Item.tolist()
                #self.timeline_vars['G'].x_range.factors = []
                #self.timeline_vars['G'].x_range.factors = sorted(DF.Start.tolist())

                timeline_source.data = data


            except Exception:
                logger.error('timeline', exc_info=True)


        def timeline_plot(self,DF):
            try:
                hover = HoverTool(tooltips="Task: @Item<br>\
                Start: @start<br>\
                End: @end")
                self.timeline_vars['G'].quad(left='Start', right='End', bottom='ID',
                                             top='ID1', source=timeline_source, color="Color")

                self.tools = [hover]+self.tools
                self.timeline_vars['G'].tools = self.tools
                self.timeline_vars['toolbar_box'] = ToolbarBox()
                self.timeline_vars['toolbar_box'].toolbar = Toolbar(tools=self.tools)
                self.timeline_vars['toolbar_box'].toolbar_location = "above"


                self.timeline_vars['G'].x_range.start = DF.Start.min() - timedelta(days=10)
                self.timeline_vars['G'].x_range.start = DF.End.max() + timedelta(days=10)

                return self.timeline_vars['G']
            except Exception:
                logger.error('timeline', exc_info=True)


    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pm_gender = pm_gender_select.value
        thistab.m_gender = m_gender_select.value
        thistab.t_gender = t_gender_select.value

        thistab.type = type_select.value
        thistab.variable = variable_select.value
        if 'project' in thistab.variable:
            thistab.groupby_var = 'project'
        elif 'milestone' in thistab.variable:
            thistab.groupby_var = 'milestone'
        elif 'task' in thistab.variable:
            thistab.groupby_var = 'task'

        thistab.status = status_select.value
        thistab.graph_periods_to_date(thistab.df, thistab.timestamp_col, variable=thistab.variable)
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_pop_dates():
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pop_history_periods = pop_number_select.value
        thistab.pop_start_date = datepicker_pop_start.value  # trigger period over period
        thistab.pop_end_date = datepicker_pop_end.value
        thistab.df_pop = thistab.pym.load_df(start_date=thistab.pop_start_date,
                                         end_date=thistab.pop_end_date,
                                         cols=[], table=thistab.table, timestamp_col='startdate_actual')
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_history_periods(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pop_history_periods = pop_number_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")


    def update_timeline(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.timeline_vars['project'] = timeline_project_select.value
        thistab.timeline_vars['type'] = timeline_type_select.value
        thistab.timeline(thistab.timeline_vars['project'],thistab.timeline_vars['type'])
        thistab.notification_updater("ready")

    try:
        cols = []
        thistab = Thistab(table='project_composite', cols=cols)
        # -------------------------------------  SETUP   ----------------------------
        # format dates
        first_date_range = thistab.initial_date
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date']
        first_date = datetime(last_date.year, 4, 1, 0, 0, 0)

        thistab.df = thistab.pym.load_df(start_date=first_date,end_date=last_date,table=thistab.table,cols=[],
                                     timestamp_col=thistab.timestamp_col)
        thistab.graph_periods_to_date(thistab.df, timestamp_filter_col=thistab.timestamp_col, variable=thistab.variable)
        thistab.pop_end_date = last_date
        thistab.pop_start_date = last_date - timedelta(days=5)
        thistab.df_pop = thistab.pym.load_df(start_date=thistab.pop_start_date,
                                             end_date=thistab.pop_end_date,
                                             cols=[], table=thistab.table,
                                             timestamp_col=thistab.timestamp_col)

        thistab.timeline_vars['projects'] = sorted(list(set(thistab.df['project'].tolist())))
        thistab.timeline_vars['project'] = thistab.timeline_vars['projects'][0]

        # MANAGE STREAM
        # date comes out stream in milliseconds
        # --------------------------------CREATE WIDGETS ---------------------------------


        stream_launch = streams.Stream.define('Launch', launch=-1)()

        datepicker_pop_start = DatePicker(title="Period start", min_date=first_date_range,
                                        max_date=last_date_range, value=thistab.pop_start_date)

        datepicker_pop_end = DatePicker(title="Period end", min_date=first_date_range,
                                        max_date=last_date_range, value=thistab.pop_end_date)

        pop_number_select = Select(title='Select # of comparative periods',
                                   value=str(thistab.pop_history_periods),
                                   options=thistab.menus['history_periods'])
        pop_dates_button = Button(label="Select dates, then click me!", width=15, button_type="success")

        type_select = Select(title='Select project type', value=thistab.type,
                                 options=thistab.menus['type'])

        status_select = Select(title='Select project status', value=thistab.status,
                                     options=thistab.menus['status'])

        pm_gender_select = Select(title="Select project owner's gender", value=thistab.pm_gender,
                                       options=thistab.menus['gender'])

        m_gender_select = Select(title="Select milestone owner's gender", value=thistab.m_gender,
                                      options=thistab.menus['gender'])

        t_gender_select = Select(title="Select task owner's gender", value=thistab.t_gender,
                                      options=thistab.menus['gender'])

        variable_select = Select(title='Select variable of interest', value=thistab.variable,
                               options=thistab.menus['variables'])

        timeline_project_select = Select(title='Select project', value=thistab.timeline_vars['project'],
                                 options=thistab.timeline_vars['projects'])

        timeline_type_select = Select(title='Select granularity', value='all',
                                         options=thistab.timeline_vars['types'])

        # ---------------------------------  GRAPHS ---------------------------
        hv_pop_week = hv.DynamicMap(thistab.pop_week, streams=[stream_launch])
        pop_week = renderer.get_plot(hv_pop_week)

        hv_pop_month = hv.DynamicMap(thistab.pop_month, streams=[stream_launch])
        pop_month = renderer.get_plot(hv_pop_month)

        hv_pop_quarter = hv.DynamicMap(thistab.pop_quarter, streams=[stream_launch])
        pop_quarter = renderer.get_plot(hv_pop_quarter)

        hv_pop_year = hv.DynamicMap(thistab.pop_year, streams=[stream_launch])
        pop_year = renderer.get_plot(hv_pop_year)

        hv_chord = hv.DynamicMap(thistab.chord_diagram, streams=[stream_launch])
        chord = renderer.get_plot(hv_chord)

        thistab.timeline(thistab.timeline_vars['project'],thistab.timeline_vars['type'])
        timeline = thistab.timeline_plot(DF=thistab.timeline_vars['DF'])

        # -------------------------------- CALLBACKS ------------------------

        type_select.on_change('value', update)
        pop_dates_button.on_click(update_pop_dates)  # lags array
        status_select.on_change('value', update)
        pm_gender_select.on_change('value', update)
        m_gender_select.on_change('value', update)
        t_gender_select.on_change('value', update)
        variable_select.on_change('value', update)
        pop_number_select.on_change('value',update_history_periods)
        timeline_project_select.on_change('value',update_timeline)
        timeline_type_select.on_change('value', update_timeline)
        # -----------------------------------LAYOUT ----------------------------
        # put the controls in a single element
        controls_top = WidgetBox(
            variable_select,type_select, status_select,pm_gender_select, m_gender_select, t_gender_select,
        )

        controls_pop = WidgetBox(datepicker_pop_start,datepicker_pop_end, pop_number_select)
        controls_timeline = WidgetBox(thistab.timeline_vars['toolbar_box'],
                                      timeline_project_select, timeline_type_select)


        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.section_headers['cards']],
            [thistab.KPI_card_div,controls_top],
            [thistab.section_headers['pop']],
            [Spacer(width=20, height=25)],
            [pop_week.state,controls_pop],
            [pop_month.state],
            [pop_quarter.state],
            [pop_year.state],
            [thistab.section_headers['chord']],
            [Spacer(width=20, height=25)],
            [chord.state],
            [thistab.section_headers['timeline']],
            [Spacer(width=20, height=25)],
            [timeline,controls_timeline],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag(panel_title)
