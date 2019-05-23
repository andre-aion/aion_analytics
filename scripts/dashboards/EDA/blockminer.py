from concurrent.futures import ThreadPoolExecutor
from os.path import join, dirname

from scripts.utils.interfaces.mytab_interface import Mytab
from scripts.utils.myutils import tab_error_flag, datetime_to_date
from scripts.utils.mylogger import mylogger

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import CustomJS, ColumnDataSource, Panel, Button, Spacer

import gc
from bokeh.models.widgets import Div, Select, \
    DatePicker, TableColumn, DataTable, Paragraph
from holoviews import streams

from datetime import datetime, timedelta

from tornado.gen import coroutine
executor = ThreadPoolExecutor(max_workers=5)


import holoviews as hv
hv.extension('bokeh',logo=False)
logger = mylogger(__file__)

menu = list()
for i in range(0, 400, 5):
    if i not in [0, 5]:
        menu.append(str(i))

@coroutine
def blockminer_tab():

    # source for top N table
    topN_src = ColumnDataSource(data=dict(percentage=[],
                                     address=[],
                                     block_count=[]))


    class This_tab(Mytab):
        def __init__(self,table,cols,dedup_cols):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.df2 = None
            self.key_tab = 'blockminer'
            self.n = 20
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

            self.section_divider = '-----------------------------------'
            self.section_headers = {

            }

            # ----- UPDATED DIVS END

            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)


        def load_this_data(self, start_date, end_date):
            end_date = datetime.combine(end_date, datetime.min.time())
            start_date = datetime.combine(start_date, datetime.min.time())

            logger.warning('load_data start date:%s', start_date)
            logger.warning('load_data end date:%s', end_date)

            # load only mined blocks and remove the double entry
            supplemental_where = "AND update_type = 'mined_block' AND amount >= 0"

            self.df_load(start_date, end_date,supplemental_where=supplemental_where,
                         cols=['address','amount','block_time'])
            logger.warning('after load:%s',self.df.head(30))

            return self.prep_dataset(start_date, end_date)


        def prep_dataset(self, start_date, end_date):
            try:
                logger.warning("prep dataset start date:%s", start_date)

                self.df1 = self.df1[['address','block_time']]
                self.df1['address'] = self.df1['address']\
                    .map(self.poolname_verbose_trun)
                self.df1 = self.df1.groupby(['address']).agg({'block_time':'count'})
                self.df1 = self.df1.reset_index()
                self.df1 = self.df1.rename(columns = {'block_time':'block_count'})
                self.df1['percentage'] = 100*self.df1['block_count']\
                                         /self.df1['block_count'].sum()
                self.df1['percentage'] = self.df1['percentage'].map(lambda x: round(x,1))
                self.df1 = self.df1.reset_index()
                logger.warning("topN column:%s",self.df1.columns.tolist())
                #logger.warning('END prep dataset DF1:%s', self.df1.head())

                return self.df1.hvplot.bar('address', 'block_count', rot=90,
                                           height= 600, width=1500, title='# of blocks mined by miner address',
                                           hover_cols=['percentage'])

            except Exception:
                logger.error('prep dataset:', exc_info=True)

        def view_topN(self):
            logger.warning("top n called:%s",self.n)
            # change n from string to int
            try:
                #table_n = df1.hvplot.table(columns=['address','percentage'],
                                          #title=title, width=400)
                logger.warning('top N:%s',self.n)
                df2 = self.df1.nlargest(self.n,'percentage')
                df2 = df2.compute()
                logger.warning('in view top n :%s',df2.head(10))

                new_data = dict(
                    percentage=df2.percentage.tolist(),
                    address=df2.address.tolist(),
                    block_count=df2.block_count.tolist()
                )
                topN_src.stream(new_data, rollover=self.n)
                columns = [
                    TableColumn(field="address", title="Address"),
                    TableColumn(field="percentage", title="percentage"),
                    TableColumn(field="block_count", title="# of blocks")
                ]

                table_n = DataTable(source=topN_src, columns=columns, width=300, height=600)

                gc.collect()
                return table_n
            except Exception:
                logger.error('view_topN:', exc_info=True)

        def set_n(self, n):
            if isinstance(n, int):
                pass
            else:
                try:
                    self.n = int(n)
                except Exception:
                    logger.error('set_n', exc_info=True)

        # ####################################################
        #              UTILITY DIVS

        def results_div(self, text, width=600, height=300):
            div = Div(text=text, width=width, height=height)
            return div

        def title_div(self, text, width=700):
            text = '<h2 style="color:green;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=20)

        def notification_updater_2(self, text):
            self.notification_div.text = '<h3  style="color:red">{}</h3>'.format(text)


        def spacing_div(self, width=20, height=100):
            return Div(text='', width=width, height=height)

        def spacing_paragraph(self, width=20, height=100):
            return Paragraph(text='', width=width, height=height)

    def update(attrname, old, new):
        this_tab.notification_updater("Calculations underway. Please be patient")
        stream_start_date.event(start_date=datepicker_start.value)
        stream_end_date.event(end_date=datepicker_end.value)
        this_tab.set_n(topN_select.value)
        this_tab.view_topN()
        this_tab.notification_updater("ready")

    # update based on selected top n
    def update_topN():
        this_tab.notification_updater("Calculations in progress! Please wait.")
        logger.warning('topN selected value:%s',topN_select.value)
        this_tab.set_n(topN_select.value)
        this_tab.view_topN()
        this_tab.notification_updater("ready")


    try:
        # create class and get date range
        cols = ['address','block_timestamp', 'block_time']
        this_tab = This_tab('account_ext_warehouse',cols, [])

        #STATIC DATES
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = last_date_range
        first_date = datetime_to_date(last_date - timedelta(days=60))

        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_start_date = streams.Stream.define('Start_date',
                                                  start_date=first_date)()
        stream_end_date = streams.Stream.define('End_date',
                                                end_date=last_date)()

        # create a text widget for top N
        topN_select = Select(title='Top N', value=str(this_tab.n), options=menu)

        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        # ALL MINERS.
        # --------------------- ALL  MINERS ----------------------------------
        hv_bar_plot = hv.DynamicMap(this_tab.load_this_data,
                                    streams=[stream_start_date,
                                             stream_end_date],
                                    datashade=True)

        renderer = hv.renderer('bokeh')
        bar_plot = renderer.get_plot(hv_bar_plot)

        # --------------------- TOP N MINERS -----------------------------------
        # set up data source for the ton N miners table
        this_tab.view_topN()
        columns = [
            TableColumn(field="address", title="Address"),
            TableColumn(field="percentage", title="percentage"),
            TableColumn(field="block_count", title="# of blocks")
        ]
        topN_table = DataTable(source=topN_src, columns=columns, width=400, height=600)

        # add callbacks
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)
        topN_select.on_change("value", lambda attr, old, new: update_topN())


        download_button = Button(label='Save Table to CSV', button_type="success")
        download_button.callback = CustomJS(args=dict(source=topN_src),
            code=open(join(dirname(__file__),
                           "../../../static/js/topN_download.js")).read())


        # put the controls in a single element
        controls = WidgetBox(datepicker_start, datepicker_end,
                             download_button, topN_select)


        # create the dashboards
        grid = gridplot([[this_tab.notification_div['top']],
                         [Spacer(width=20, height=70)],
                         [topN_table,controls,],
                         [bar_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='miners: blocks')

        return tab

    except Exception:
        logger.error("Blockminer", exc_info=True)

        return tab_error_flag('miners: blocks')