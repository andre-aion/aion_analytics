import time

from scripts.utils.mylogger import mylogger
from scripts.utils.dashboard.churned_model_tab import ChurnedModelTab
from scripts.utils.myutils import tab_error_flag
from scripts.streaming.streamingDataframe import StreamingDataframe as SD

from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import ColumnDataSource, Panel
from bokeh.models.widgets import  Div, \
    DatePicker, Select

from datetime import datetime
from holoviews import streams
import holoviews as hv
import hvplot.pandas
import hvplot.dask
from tornado.gen import coroutine
from config import load_columns


lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)

@coroutine
def churned_model(tier=1):
    class Thistab(ChurnedModelTab):

        def __init__(self,cols):
            ChurnedModelTab.__init__(self,cols=cols)
            self.cols = cols



    def update():
        notification_div.text = thistab.notification_updater('please wait, calculations ongoing')
        thistab.load_data()
        stream_launch.event(launch=True)
        stream_select_variable.event(variable=thistab.select_variable.value)
        #hypothesis_div.text = thistab.hypothesis_test(thistab.select_variable.value)
        notification_div.text = thistab.notification_updater("")

    def update_plots(attr,old,new):
        notification_div.text = thistab.notification_updater('updating plot(s) calculations ongoing')
        #hypothesis_div.text = thistab.hypothesis_test(thistab.select_variable.value)
        stream_select_variable.event(variable=thistab.select_variable.value)
        notification_div.text = thistab.notification_updater("")

    try:
        # setup
        thistab = Thistab(cols=load_columns['block_tx_warehouse']['churn'])
        notification_text = thistab.notification_updater("")
        notification_div = Div(text=notification_text, width=500, height=50)
        thistab.make_checkboxes()
        thistab.load_data()
        stream_launch = streams.Stream.define('Launch',launch=True)()
        stream_select_variable = streams.Stream.define('Select_variable',
                                                    variable='approx_value')()

        launch_button = thistab.make_button('Launch model')
        refresh_checkbox_button = thistab.make_button('Refresh checkboxes')

        thistab.select_variable = thistab.make_selector('Choose variable','approx_value')
        #hyp_text = thistab.hypothesis_test(thistab.select_variable.value)

        # PLOTS
        hv_plot1 = hv.DynamicMap(thistab.box_plot,
                                 streams=[stream_select_variable,
                                          stream_launch])

        #hypothesis_div = thistab.results_div(text=hyp_text)
        hv_hypothesis_table = hv.DynamicMap(thistab.hypothesis_table,
                                            streams=[stream_launch])

        renderer = hv.renderer('bokeh')
        difficulty = renderer.get_plot(hv_plot1)
        hypothesis_table = renderer.get_plot(hv_hypothesis_table)

        # handle callbacks
        launch_button.on_click(update)
        refresh_checkbox_button.on_click(thistab.update_checkboxes)
        thistab.select_variable.on_change('value',update_plots)


        # organize layout
        controls = WidgetBox(thistab.checkbox_group,
                             refresh_checkbox_button,
                             thistab.select_variable,
                             launch_button)

        grid = gridplot([[controls, notification_div],
                         [difficulty.state],
                         [hypothesis_table.state]
                        ])

        tab = Panel(child=grid, title='Tier '+str(tier)+' pre churn model ')
        return tab


    except Exception:
        logger.error('rendering err:',exc_info=True)
        return tab_error_flag('tier1_churned_model')




