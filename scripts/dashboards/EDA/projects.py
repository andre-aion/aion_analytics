from datetime import datetime, timedelta

from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, Select, TableColumn, ColumnDataSource, DataTable, \
    Spacer

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.databases.pythonMongo import PythonMongo
from scripts.utils.interfaces.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from config.dashboard import config as dashboard_config
from bokeh.models.widgets import TextInput

from tornado.gen import coroutine

import pandas as pd
import holoviews as hv
from holoviews import streams

from scripts.utils.myutils import tab_error_flag

logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')


@coroutine
def eda_projects_tab(panel_title):
    lags_corr_src = ColumnDataSource(data=dict(
        variable_1=[],
        variable_2=[],
        relationship=[],
        lag=[],
        r=[],
        p_value=[]
    ))

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
            self.groupby_dict = {
                'project_duration': 'sum',
                'project_start_delay': 'mean',
                'project_end_delay': 'mean',
                'project_owner_age':'mean',
                'project_owner_gender':'mean',

                'milestone_duration': 'sum',
                'milestone_start_delay': 'mean',
                'milestone_end_delay': 'mean',
                'milestone_owner_age': 'mean',
                'milestone_owner_gender': 'mean',

                'task_duration': 'sum',
                'task_start_delay': 'sum',
                'task_end_delay': 'mean',
                'task_owner_age': 'mean',
                'task_owner_gender': 'mean'
            }
            self.feature_list = list(self.groupby_dict.keys())
            self.lag_variable = 'task_duration'
            self.lag_days = "1,2,3"
            self.lag = 0
            self.lag_menu = [str(x) for x in range(0, 100)]

            self.strong_thresh = .65
            self.mod_thresh = 0.4
            self.weak_thresh = 0.25
            self.corr_df = None
            self.div_style = """ 
                style='width:350px; margin-left:25px;
                border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
            """

            self.header_style = """ style='color:blue;text-align:center;' """

            self.variables = sorted(list(self.groupby_dict.keys()))
            self.variable = self.variables[0]



            self.relationships_to_check = ['weak', 'moderate', 'strong']

            self.status = 'all'
            self.pm_gender = 'all'
            self.m_gender = 'all'
            self.t_gender = 'all'
            self.type = 'all'

            self.pym = PythonMongo('aion')
            self.menus = {
                'status': ['all', 'open', 'closed'],
                'type': ['all', 'research', 'reconciliation', 'audit', 'innovation', 'construction', 'manufacturing',
                         'conference'],
                'gender': ['all', 'male', 'female'],
                'variables': list(self.groupby_dict.keys()),
                'history_periods': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
            }
            self.multiline_vars = {
                'x':'manager_gender',
                'y':'remuneration'
            }
            self.timestamp_col = 'project_startdate_actual'
            # ------- DIVS setup begin
            self.page_width = 1250
            txt = """<hr/>
                    <div style="text-align:center;width:{}px;height:{}px;
                           position:relative;background:black;margin-bottom:200px">
                           <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                    </div>""".format(self.page_width, 50, 'Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=self.page_width, height=20),
                'bottom': Div(text=txt, width=self.page_width, height=10),
            }
            lag_section_head_txt = 'Lag relationships between {} and...'.format(self.variable)

            self.section_divider = '-----------------------------------'
            self.section_headers = {

                'lag': self.section_header_div(text=lag_section_head_txt,
                                               width=600, html_header='h2', margin_top=5,
                                               margin_bottom=-155),
                'distribution': self.section_header_div(text='Pre-transform distribution:',
                                                 width=600, html_header='h2', margin_top=5,
                                                 margin_bottom=-155),
                'relationships': self.section_header_div(
                    text='Relationships between variables:{}'.format(self.section_divider),
                    width=600, html_header='h2', margin_top=5,
                    margin_bottom=-155),
                'correlations': self.section_header_div(
                    text='Correlations:',
                    width=600, html_header='h3', margin_top=5,
                    margin_bottom=-155),

            }

            # ----- UPDATED DIVS END

            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                    <h4 style="color:#fff;">
                    {}</h4></div>""".format(text)
            for key in self.notification_div.keys():
                self.notification_div[key].text = txt

        def reset_adoption_dict(self, variable):
            self.significant_effect_dict[variable] = []

        # //////////////  DIVS   /////////////////////////////////

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        def corr_information_div(self, width=400, height=300):
            div_style = """ 
                style='width:350px; margin-left:-600px;
                border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
            """
            txt = """
            <div {}>
            <h4 {}>How to interpret relationships </h4>
            <ul style='margin-top:-10px;'>
                <li>
                Positive: as variable 1 increases, so does variable 2.
                </li>
                <li>
                Negative: as variable 1 increases, variable 2 decreases.
                </li>
                <li>
                Strength: decisions can be made on the basis of strong and moderate relationships.
                </li>
                <li>
                No relationship/not significant: no statistical support for decision making.
                </li>
                 <li>
               The scatter graphs (below) are useful for visual confirmation.
                </li>
                 <li>
               The histogram (right) shows the distribution of the variable.
                </li>
            </ul>
            </div>

            """.format(div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        # /////////////////////////////////////////////////////////////
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

        def prep_data(self, df1):
            try:
                '''
                df1[self.timestamp_col] = df1[self.timestamp_col].apply(lambda x: datetime(x.year,
                                                                                   x.month,
                                                                                   x.day,
                                                                                   x.hour,0,0))
                '''
                df1 = df1.set_index(self.timestamp_col)
                logger.warning('LINE 195 df:%s', df1.head())
                # handle lag for all variables

                df = df1.copy()
                df = self.filter_df(df)

                logger.warning('LINE 199: length before:%s', len(df))
                slice = df[['project']]
                df = df[list(self.groupby_dict.keys())]
                logger.warning('LINE 218: columns:%s',df.head())
                df = df.astype(float)
                df = pd.concat([df,slice],axis=1)
                df = df.groupby('project').resample(self.resample_period).agg(self.groupby_dict)
                logger.warning('LINE 201: length after:%s', len(df))

                df = df.reset_index()
                vars = self.feature_list.copy()
                if int(self.lag) > 0:
                    for var in vars:
                        if self.variable != var:
                            df[var] = df[var].shift(int(self.lag))
                df = df.dropna()
                self.df1 = df
                logger.warning('line 184- prep data: df:%s', self.df.head(10))

            except Exception:
                logger.error('prep data', exc_info=True)

        def lags_plot(self, launch):
            try:
                df = self.df.copy()
                df = df[[self.lag_variable, self.variable]]
                cols = [self.lag_variable]
                lags = self.lag_days.split(',')
                for day in lags:
                    try:
                        label = self.lag_variable + '_' + day
                        df[label] = df[self.lag_variable].shift(int(day))
                        cols.append(label)
                    except:
                        logger.warning('%s is not an integer', day)
                df = df.dropna()
                self.lags_corr(df)
                # plot the comparison
                logger.warning('in lags plot: df:%s', df.head(10))
                return df.hvplot(x=self.variable, y=cols, kind='scatter', alpha=0.4)
            except Exception:
                logger.error('lags plot', exc_info=True)

        # calculate the correlation produced by the lags vector
        def lags_corr(self, df):
            try:
                corr_dict_data = {
                    'variable_1': [],
                    'variable_2': [],
                    'relationship': [],
                    'lag': [],
                    'r': [],
                    'p_value': []
                }
                a = df[self.variable].tolist()
                for col in df.columns:
                    if col not in [self.timestamp_col, self.variable]:
                        # find lag
                        var = col.split('_')
                        try:
                            tmp = int(var[-1])

                            lag = tmp
                        except Exception:
                            lag = 'None'

                        b = df[col].tolist()
                        slope, intercept, rvalue, pvalue, txt = self.corr_label(a, b)
                        corr_dict_data['variable_1'].append(self.variable)
                        corr_dict_data['variable_2'].append(col)
                        corr_dict_data['relationship'].append(txt)
                        corr_dict_data['lag'].append(lag)
                        corr_dict_data['r'].append(round(rvalue, 4))
                        corr_dict_data['p_value'].append(round(pvalue, 4))

                lags_corr_src.stream(corr_dict_data, rollover=(len(corr_dict_data['lag'])))
                columns = [
                    TableColumn(field="variable_1", title="variable 1"),
                    TableColumn(field="variable_2", title="variable 2"),
                    TableColumn(field="relationship", title="relationship"),
                    TableColumn(field="lag", title="lag(days)"),
                    TableColumn(field="r", title="r"),
                    TableColumn(field="p_value", title="p_value"),

                ]
                data_table = DataTable(source=lags_corr_src, columns=columns, width=500, height=280)
                return data_table
            except Exception:
                logger.error('lags corr', exc_info=True)

        def correlation_table(self, launch):
            try:

                corr_dict = {
                    'Variable 1': [],
                    'Variable 2': [],
                    'Relationship': [],
                    'r': [],
                    'p-value': []
                }
                # prep df
                df = self.df1
                # get difference for money columns
                df = df.drop(self.timestamp_col, axis=1)
                # df = df.compute()

                a = df[self.variable].tolist()

                for col in self.feature_list:
                    logger.warning('col :%s', col)
                    if col != self.variable:
                        logger.warning('%s:%s', col, self.variable)
                        b = df[col].tolist()
                        slope, intercept, rvalue, pvalue, txt = self.corr_label(a, b)
                        # add to dict
                        corr_dict['Variable 1'].append(self.variable)
                        corr_dict['Variable 2'].append(col)
                        corr_dict['Relationship'].append(txt)
                        corr_dict['r'].append(round(rvalue, 4))
                        corr_dict['p-value'].append(round(pvalue, 4))

                df = pd.DataFrame(
                    {
                        'Variable 1': corr_dict['Variable 1'],
                        'Variable 2': corr_dict['Variable 2'],
                        'Relationship': corr_dict['Relationship'],
                        'r': corr_dict['r'],
                        'p-value': corr_dict['p-value']

                    })
                # logger.warning('df:%s',df.head(23))
                return df.hvplot.table(columns=['Variable 1', 'Variable 2', 'Relationship', 'r', 'p-value'],
                                       width=550, height=200, title='Correlation between variables')
            except Exception:
                logger.error('correlation table', exc_info=True)

        def non_parametric_relationship_table(self, launch):
            try:

                corr_dict = {
                    'Variable 1': [],
                    'Variable 2': [],
                    'Relationship': [],
                    'stat': [],
                    'p-value': []
                }
                # prep df
                df = self.df1
                # get difference for money columns
                df = df.drop(self.timestamp_col, axis=1)
                # df = df.compute()

                # logger.warning('line df:%s',df.head(10))
                a = df[self.variable].tolist()
                for col in self.feature_list:
                    logger.warning('col :%s', col)
                    if col != self.variable:
                        logger.warning('%s:%s', col, self.variable)
                        b = df[col].tolist()
                        stat, pvalue, txt = self.mann_whitneyu_label(a, b)
                        corr_dict['Variable 1'].append(self.variable)
                        corr_dict['Variable 2'].append(col)
                        corr_dict['Relationship'].append(txt)
                        corr_dict['stat'].append(round(stat, 4))
                        corr_dict['p-value'].append(round(pvalue, 4))

                df = pd.DataFrame(
                    {
                        'Variable 1': corr_dict['Variable 1'],
                        'Variable 2': corr_dict['Variable 2'],
                        'Relationship': corr_dict['Relationship'],
                        'stat': corr_dict['stat'],
                        'p-value': corr_dict['p-value']

                    })
                # logger.warning('df:%s',df.head(23))
                return df.hvplot.table(columns=['Variable 1', 'Variable 2', 'Relationship', 'stat', 'p-value'],
                                       width=550, height=200, title='Non parametric relationship between variables')
            except Exception:
                logger.error('non parametric table', exc_info=True)

        def hist(self, launch):
            try:

                return self.df.hvplot.hist(
                    y=self.feature_list, subplots=True, shared_axes=False,
                    bins=25, alpha=0.3, width=300).cols(4)
            except Exception:
                logger.warning('histogram', exc_info=True)

        def matrix_plot(self, launch=-1):
            try:
                logger.warning('line 306 self.feature list:%s', self.feature_list)

                df = self.df1


                # thistab.prep_data(thistab.df)
                if self.timestamp_col in df.columns:
                    df = df.drop(self.timestamp_col, axis=1)

                df = df.fillna(0)
                # logger.warning('line 302. df: %s',df.head(10))

                cols_temp = self.feature_list.copy()
                if self.variable in cols_temp:
                    cols_temp.remove(self.variable)
                # variable_select.options = cols_lst

                p = df.hvplot.scatter(x=self.variable, y=cols_temp, width=330,
                                      subplots=True, shared_axes=False, xaxis=False).cols(4)

                return p

            except Exception:
                logger.error('matrix plot', exc_info=True)


        def multiline(self,launch=1):
            try:
                yvar = self.multiline_vars['y']
                xvar = self.multiline_vars['x']
                df = self.df.copy()
                df = df[[xvar,yvar,self.timestamp_col]]
                df = df.set_index(self.timestamp_col)
                df = df.groupby(xvar).resample(self.resample_period).agg({yvar:'mean'})
                df = df.reset_index()
                lines = df[xvar].unique()
                # split data frames
                dfs = {}
                for idx,line in enumerate(lines):
                    dfs[line] = df[df[xvar] == line]
                    dfs[line] = dfs[line].fillna(0)
                    logger.warning('LINE 428:%s - %s:',line,dfs[line].head())
                    if idx == 0:
                        p = dfs[line].hvplot.line(x=self.timestamp_col,y=yvar,width=1200,height=500).relabel(line)
                    else:
                        p *= dfs[line].hvplot.line(x=self.timestamp_col,y=yvar,width=2,height=500).relabel(line)
                return p
            except Exception:
                logger.error('multiline plot', exc_info=True)


    def update_variable(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data(thistab.df)
        if 'milestone owner gender' == new:
            thistab.variable = 'm_gender_code'
        if 'project owner gender' == new:
            thistab.variable = 'pm_gender_code'
        if 'task owner gender' == new:
            thistab.variable = 't_gender_code'

        if thistab.variable in thistab.adoption_variables['developer']:
            thistab.reset_adoption_dict(thistab.variable)
        thistab.section_head_updater('lag', thistab.variable)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_lag_plot_variable(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.lag_variable = new
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_lags_var.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_IVs(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.pm_gender = pm_gender_select.value
        thistab.m_gender = m_gender_select.value
        thistab.t_gender = t_gender_select.value
        thistab.status = status_select.value
        thistab.type = type_select.value
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_lag(attr, old, new):  # update lag & cryptocurrency
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.lag = int(lag_select.value)
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.df = thistab.pym.load_df(start_date=datepicker_start.value,
                                             end_date=datepicker_end.value,
                                             cols=[], table=thistab.table, timestamp_col=thistab.timestamp_col)
        thistab.df['project_owner_gender'] = thistab.df['project_owner_gender'].apply(
            lambda x: 1 if x == 'male' else 2)
        thistab.df['milestone_owner_gender'] = thistab.df['milestone_owner_gender'].apply(
            lambda x: 1 if x == 'male' else 2)
        thistab.df['task_owner_gender'] = thistab.df['task_owner_gender'].apply(
            lambda x: 1 if x == 'male' else 2)
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_resample(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.resample_period = new
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_lags_selected():
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.lag_days = lags_input.value
        logger.warning('line 381, new checkboxes: %s', thistab.lag_days)
        thistab.trigger += 1
        stream_launch_lags_var.event(launch=thistab.trigger)
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_multiline(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.multiline_vars['x'] = multiline_x_select.value
        thistab.multiline_vars['y'] = multiline_y_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    try:
        # SETUP
        table = 'project_composite'
        thistab = Thistab(table, [], [])

        # setup dates
        first_date_range = datetime.strptime("2013-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date'] - timedelta(days=2)
        first_date = last_date - timedelta(days=30)
        # initial function call
        thistab.df = thistab.pym.load_df(start_date=first_date,
                                         end_date=last_date,
                                         cols=[], table=thistab.table, timestamp_col=thistab.timestamp_col)
        thistab.df['manager_gender'] = thistab.df['project_owner_gender']
        thistab.df['project_owner_gender'] = thistab.df['project_owner_gender'].apply(
            lambda x: 1 if x == 'male' else 2)
        thistab.df['milestone_owner_gender'] = thistab.df['milestone_owner_gender'].apply(
            lambda x: 1 if x == 'male' else 2)
        thistab.df['task_owner_gender'] = thistab.df['task_owner_gender'].apply(
            lambda x: 1 if x == 'male' else 2)
        logger.warning('LINE 527:columns %s',list(thistab.df.columns))

        thistab.prep_data(thistab.df)

        # MANAGE STREAM
        stream_launch_hist = streams.Stream.define('Launch', launch=-1)()
        stream_launch_matrix = streams.Stream.define('Launch_matrix', launch=-1)()
        stream_launch_corr = streams.Stream.define('Launch_corr', launch=-1)()
        stream_launch_lags_var = streams.Stream.define('Launch_lag_var', launch=-1)()
        stream_launch = streams.Stream.define('Launch', launch=-1)()

        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)

        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        variable_select = Select(title='Select variable',
                                 value=thistab.variable,
                                 options=thistab.variables)

        lag_variable_select = Select(title='Select lag variable',
                                     value=thistab.lag_variable,
                                     options=thistab.feature_list)

        lag_select = Select(title='Select lag',
                            value=str(thistab.lag),
                            options=thistab.lag_menu)

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

        resample_select = Select(title='Select resample period',
                                 value='D', options=['D', 'W', 'M', 'Q'])

        multiline_y_select = Select(title='Select comparative DV(y)',
                                    value=thistab.multiline_vars['y'],
                                    options=['remuneration','delay_start','delay_end','project_duration'])

        multiline_x_select = Select(title='Select comparative IV(x)',
                                    value=thistab.multiline_vars['x'],
                                    options=['manager_gender', 'type','status'])

        lags_input = TextInput(value=thistab.lag_days, title="Enter lags (integer(s), separated by comma)",
                               height=55, width=300)
        lags_input_button = Button(label="Select lags, then click me!", width=10, button_type="success")

        # --------------------- PLOTS----------------------------------
        columns = [
            TableColumn(field="variable_1", title="variable 1"),
            TableColumn(field="variable_2", title="variable 2"),
            TableColumn(field="relationship", title="relationship"),
            TableColumn(field="lag", title="lag(days)"),
            TableColumn(field="r", title="r"),
            TableColumn(field="p_value", title="p_value"),

        ]
        lags_corr_table = DataTable(source=lags_corr_src, columns=columns, width=500, height=200)

        hv_matrix_plot = hv.DynamicMap(thistab.matrix_plot,
                                       streams=[stream_launch_matrix])
        hv_corr_table = hv.DynamicMap(thistab.correlation_table,
                                      streams=[stream_launch_corr])
        hv_nonpara_table = hv.DynamicMap(thistab.non_parametric_relationship_table,
                                         streams=[stream_launch_corr])
        # hv_hist_plot = hv.DynamicMap(thistab.hist, streams=[stream_launch_hist])
        hv_lags_plot = hv.DynamicMap(thistab.lags_plot, streams=[stream_launch_lags_var])
        hv_multiline = hv.DynamicMap(thistab.multiline,streams=[stream_launch])
        
        

        matrix_plot = renderer.get_plot(hv_matrix_plot)
        corr_table = renderer.get_plot(hv_corr_table)
        nonpara_table = renderer.get_plot(hv_nonpara_table)
        lags_plot = renderer.get_plot(hv_lags_plot)
        multiline = renderer.get_plot(hv_multiline)

        # setup divs

        # handle callbacks
        variable_select.on_change('value', update_variable)
        lag_variable_select.on_change('value', update_lag_plot_variable)
        lag_select.on_change('value', update_lag)  # individual lag
        resample_select.on_change('value', update_resample)
        pm_gender_select.on_change('value', update_IVs)
        m_gender_select.on_change('value', update_IVs)
        t_gender_select.on_change('value', update_IVs)
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)
        lags_input_button.on_click(update_lags_selected)  # lags array

        status_select.on_change('value', update_IVs)
        type_select.on_change('value', update_IVs)

        multiline_x_select.on_change('value',update_multiline)
        multiline_y_select.on_change('value',update_multiline)

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls_lag = WidgetBox(
            lags_input,
            lags_input_button,
            lag_variable_select
        )

        controls_multiline = WidgetBox(
            multiline_x_select,
            multiline_y_select
        )

        controls_page = WidgetBox(
            datepicker_start, datepicker_end, variable_select,
            type_select,status_select, resample_select,
            pm_gender_select, m_gender_select, t_gender_select
        )
        controls_gender =  WidgetBox(
            pm_gender_select, m_gender_select, t_gender_select
        )

        # create the dashboards

        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.section_headers['relationships']],
            [Spacer(width=20, height=70)],
            [matrix_plot.state, controls_page],
            [thistab.section_headers['correlations']],
            [Spacer(width=20, height=70)],
            [corr_table.state,thistab.corr_information_div()],
            [thistab.title_div('Compare levels in a variable', 400)],
            [Spacer(width=20, height=70)],
            [multiline.state,controls_multiline],
            [thistab.section_headers['lag']],
            [Spacer(width=20, height=70)],
            [lags_plot.state, controls_lag],
            [lags_corr_table],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('EDA projects:', exc_info=True)
        return tab_error_flag(panel_title)
