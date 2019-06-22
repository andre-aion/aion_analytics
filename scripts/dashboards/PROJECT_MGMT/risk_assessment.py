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

logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

@coroutine
def pm_risk_assessment_tab(panel_title):
    risk_matrix_src = ColumnDataSource(data=dict(
        Severity=[],
        Unlikely=[],
        Seldom=[],
        Occaisional=[],
        Likely=[],
        Definite=[]
    ))
    
    corr_src = ColumnDataSource(data=dict(
            variable_1= [],
            variable_2= [],
            relationship= [],
            r= [],
            p_value= []
        )
    )

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
            self.variable = 'delay_end'


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
            self.risk_threshold = {
                'acceptable' : 8,
                'doubtful' : 15
            }

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
            lag_section_head_txt = 'Lag relationships between {} and...'.format(self.variable)
            self.section_divider = '-----------------------------------'
            self.section_headers = {
                'lag': self.section_header_div(text=lag_section_head_txt,
                                                 width=1000, html_header='h2', margin_top=50, margin_bottom=5),
                'distribution': self.section_header_div(text='Pre-transform distribution',
                                               width=600, html_header='h2', margin_top=5, margin_bottom=-155),
                'matrix':self.section_header_div(text='Risk Matrix:{}'.format(self.section_divider),
                                               width=600, html_header='h2', margin_top=5, margin_bottom=-155),
                'risk_solution':self.section_header_div(text='Risk Matrix vs Solution :{}'.format(self.section_divider),
                                               width=600, html_header='h2', margin_top=5, margin_bottom=-155),
            }


            # ----- UPDATED DIVS END

            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def load_df(self):
            try:
                risk_matrx = json_normalize(list(self.pym.db['risk_matrix'].find()))
                logger.warning('LINE 169:RISK MATIRX:%s', risk_matrx.head())
                if len(risk_matrx) > 0:
                    risk_matrx = drop_cols(risk_matrx,['desc'])
                    logger.warning('LINE 159:RISK MATIRX:%s', risk_matrx.head())

                    risk = json_normalize(list(self.pym.db['risk'].find()))
                    risk = risk.rename(columns={'matrix':'matrix_id'})
                    analysis = json_normalize(list(self.pym.db['risk_analysis'].find()))
                    analysis = drop_cols(analysis,['_id'])
                    analysis = analysis.rename(columns={'risk':'risk_id'})

                    # merges
                    risk = risk.merge(analysis,how='inner',left_on='_id',right_on='risk_id')
                    risk = drop_cols(risk,['_id','likelihood_comment','severity_comment','desc','risk_id'])
                    logger.warning('LINE 167:RISK:%s', risk.head())
                    logger.warning('LINE 169:RISK MATIRX:%s', risk_matrx.head())

                    risk = risk_matrx.merge(risk,how='inner',left_on='_id',right_on='matrix_id')

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
                    if x < self.risk_threshold['acceptable']:
                        return 'Proceed (risk is acceptable)'
                    elif x < self.risk_threshold['doubtful']:
                        return 'Proceed, if no other options are available'
                    else:
                        return 'Do no proceed (Risk unacceptable)'
                df = self.df
                df = df.groupby(['matrix', 'risk']).agg({'likelihood': 'mean', 'severity': 'mean'})
                df = df.reset_index()
                df['composite'] = df.likelihood * df.severity
                df['action'] = df['composite'].map(label_action)
                self.risk_matrix()
                return df.hvplot.table(columns=['matrix', 'risk', 'severity', 'likelihood','action'],width=1000,
                                       )
            except Exception:
                logger.error('action table', exc_info=True)

        def risk_matrix(self):
            try:
                # filter
                df = self.df1
                df = df.groupby(['matrix', 'risk']).agg({'likelihood': 'mean', 'severity': 'mean'})
                df = df.reset_index()
                df = df[df['risk'] == self.risk]
                severity_value = int(df['severity'].mean())
                #severity = [key for (key, value) in self.ratings['severity'].items() if value == severity_value][0]
                likelihood_value =  int(df['likelihood'].mean())
                logger.warning('severity=%s,likelihood=%s',severity_value,likelihood_value)

                # make the matrix
                dct = {
                    'Severity':list(self.ratings['severity'].keys()),
                }
                cols = list(self.ratings['likelihood'].keys())

                for idx_row, val_col in enumerate(list(self.ratings['likelihood'].keys())):
                    row = idx_row + 1
                    dct[val_col] = []
                    for idx_row, val_row in enumerate(dct['Severity']):
                        col = idx_row + 1
                        val = row * col
                        if row == severity_value and col == likelihood_value:
                            logger.warning('CONDITIONS MET')
                            txt = 'BINGO '+str(val)
                        else:
                            txt = val

                        dct[val_col].append(txt)

                logger.warning('LINE 288 %s - length=%s',val_col,len(dct[val_col]))

                risk_matrix_src.stream(dct, rollover=(len(dct['Severity'])))
                columns = [
                    TableColumn(field="Severity",title='severity'),
                    TableColumn(field="Unlikely", title='unlikely',formatter=dashboard_config['formatters']['Unlikely']),
                    TableColumn(field="Seldom", title='seldom', formatter=dashboard_config['formatters']['Seldom']),
                    TableColumn(field="Occaisional", title='occaisional',
                                formatter=dashboard_config['formatters']['Occaisional']),
                    TableColumn(field="Likely", title='likely', formatter=dashboard_config['formatters']['Likely']),
                    TableColumn(field="Definite", title='definite',formatter=dashboard_config['formatters']['Definite']),
                ]
                risk_matrix_table = DataTable(source=risk_matrix_src, columns=columns,
                                              width=800, height=500)
                self.corr()
                return risk_matrix_table
            except Exception:
                logger.error('risk matrix', exc_info=True)

        def correlate_solution_risk(self, launch):
            try:
                # load solution
                df = json_normalize(list(self.pym.db['project_composite1'].
                                         find({}, {'severity': 1, 'likelihood': 1, 'solution': 1,
                                                   'project_owner_gender': 1, 'project': 1})))
                df['solution'] = df.solution.apply(lambda x: x[0]*10 )

                df = df.groupby(['project']).agg({'severity': 'mean', 'likelihood': 'mean', 'solution': 'mean'})
                df = df.reset_index()
                df['composite'] = df.severity * df.likelihood
                logger.warning('df:%s', df.head(20))

                # load project
                for idx, col in enumerate(['severity', 'likelihood', 'composite']):
                    if idx == 0:
                        p = df.hvplot.scatter(x='solution',y=col)
                    else:
                        p *= df.hvplot.scatter(x='solution',y=col)
                return p
                # load risk
            except Exception:
                logger.error('correlate solution risk', exc_info=True)

        def risk_information_div(self, width=400, height=300):
            txt = """
                   <div {}>
                   <h4 {}>How to interpret Risk assessment matrix:</h4>
                   <ul style='margin-top:-10px;'>
                       <li>
                       Red: Unacceptable risk. Do NOT proceed.
                       </li>
                       <li>
                       Yellow: Risky. Proceed only after ensuring better options aren't reasonable available
                       </li>
                       <li>
                       Green: Acceptable risk. Proceed.
                       </li>
                   </ul>
                   </div>
    
                   """.format(self.div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        # calculate the correlation produced by the lags vector
        def corr(self):
            try:
                corr_dict_data = {
                    'variable_1': [],
                    'variable_2': [],
                    'relationship': [],
                    'r': [],
                    'p_value': []
                }
                # load solution
                df = json_normalize(list(self.pym.db['project_composite1'].
                                         find({}, {'severity': 1, 'likelihood': 1, 'solution': 1,
                                                   'project_owner_gender': 1, 'project': 1})))
                df['solution'] = df.solution.apply(lambda x: x[0] * 10)

                df = df.groupby(['project']).agg({'severity': 'mean', 'likelihood': 'mean', 'solution': 'mean'})
                df = df.reset_index()
                df['composite'] = df.severity * df.likelihood
                logger.warning('df:%s', df.head(20))

                a = df['solution'].tolist()
                for col in ['composite','severity','likelihood']:
                    # find lag
                    logger.warning('column:%s',col)
                    b = df[col].tolist()
                    slope, intercept, rvalue, pvalue, txt = self.corr_label(a, b)
                    corr_dict_data['variable_1'].append('solution')
                    corr_dict_data['variable_2'].append(col)
                    corr_dict_data['relationship'].append(txt)
                    corr_dict_data['r'].append(round(rvalue, 3))
                    corr_dict_data['p_value'].append(round(pvalue, 3))

                corr_src.stream(corr_dict_data, rollover=3)
                columns = [
                    TableColumn(field="variable_1", title="variable 1"),
                    TableColumn(field="variable_2", title="variable 2"),
                    TableColumn(field="relationship", title="relationship"),
                    TableColumn(field="r", title="r"),
                    TableColumn(field="p_value", title="p_value"),

                ]
                data_table = DataTable(source=corr_src, columns=columns, width=900, height=400)
                return data_table
            except Exception:
                logger.error(' corr', exc_info=True)

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
        thistab.risk_matrix()
        thistab.notification_updater("Ready!")

    try:
        # SETUP
        table = 'project_composite'
        thistab = Thistab(table, [], [])
        thistab.load_df()
        thistab.corr()

        # MANAGE STREAM
        stream_launch_action_table = streams.Stream.define('Launch', launch=-1)()
        stream_launch_matrix = streams.Stream.define('Launch', launch=-1)()
        stream_launch_risk_solution = streams.Stream.define('Launch', launch=-1)()

        # MAKE TABLES
        # --------------------- PLOTS----------------------------------
        columns = [
            TableColumn(field="Severity",title="severity"),
            TableColumn(field="Unlikely",title='unlikely',formatter=dashboard_config['formatters']['Unlikely']),
            TableColumn(field="Seldom",title='seldom',formatter=dashboard_config['formatters']['Seldom']),
            TableColumn(field="Occaisional",title='occaisional',formatter=dashboard_config['formatters']['Occaisional']),
            TableColumn(field="Likely",title='likely',formatter=dashboard_config['formatters']['Likely']),
            TableColumn(field="Definite",title='definite',formatter=dashboard_config['formatters']['Definite']),
        ]

        risk_matrix = DataTable(source=risk_matrix_src, columns=columns, width=800, height=500)

        columns = [
            TableColumn(field="variable_1", title="variable 1"),
            TableColumn(field="variable_2", title="variable 2"),
            TableColumn(field="relationship", title="relationship"),
            TableColumn(field="r", title="r"),
            TableColumn(field="p_value", title="p_value"),

        ]
        corr_table = DataTable(source=corr_src, columns=columns, width=500, height=280)

        width = 800


        hv_action_table = hv.DynamicMap(thistab.action_table,
                                      streams=[stream_launch_action_table])
        action_table = renderer.get_plot(hv_action_table)

        hv_risk_solution = hv.DynamicMap(thistab.correlate_solution_risk,
                                       streams=[stream_launch_risk_solution])
        risk_solution = renderer.get_plot(hv_risk_solution)

        # CREATE WIDGETS
        matrix_select = Select(title='Select matrix', value=thistab.matrix,
                               options=thistab.matrices)

        # handle callbacks
        matrix_select.on_change('value', update_matrix)
        thistab.risk_select.on_change('value', update_risk)

        # create the dashboards
        controls = WidgetBox(matrix_select,thistab.risk_select)

        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.title_div('Determine action', 400)],
            [Spacer(width=20, height=30)],
            [action_table.state],
            [thistab.section_headers['matrix']],
            [Spacer(width=20, height=30)],
            [risk_matrix,controls],
            [thistab.section_headers['risk_solution']],
            [Spacer(width=20, height=30)],
            [corr_table],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('EDA projects:', exc_info=True)
        return tab_error_flag(panel_title)