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
def pm_business_events_tab(panel_title):
    corr_src = ColumnDataSource(data=dict(
        variable_1=[],
        variable_2=[],
        relationship=[],
        r=[],
        p_value=[]
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
            self.risk_select = Select(title='Select risk', value=self.risk, options=self.risks)
            self.risk_threshold = {
                'acceptable': 8,
                'doubtful': 15
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
            self.section_divider = '-----------------------------------'
            self.section_headers = {
                'Customers':self.section_header_div(text='Events:',
                                               width=1000, html_header='h2', margin_top=50, margin_bottom=-155),
                'Events': self.section_header_div(text='Events:',
                                               width=1000, html_header='h2', margin_top=50, margin_bottom=-155),
                'Patrons': self.section_header_div(text='Patrons:',
                                                        width=600, html_header='h2', margin_top=5, margin_bottom=-155),
                'Employees': self.section_header_div(text='Employees:'.format(self.section_divider),
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
                
            except Exception:
                logger.error('',exc_info=True)
