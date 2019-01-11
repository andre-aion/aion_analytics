from scripts.utils.mylogger import mylogger
from scripts.utils.dashboard.churned_model_tab import ChurnedModelTab
from scripts.utils.myutils import tab_error_flag
from scripts.utils.dashboard.mytab import Mytab
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



lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)

@coroutine
def churned_model(tier=1):
    class Thistab(ChurnedModelTab):

        def __init__(self):
            ChurnedModelTab.__init__(self)

        def difficulty_plot(self,launch_plots=False):
            return self.df.hvplot.hist(
                'difficulty', by='state', bins=20, bin_range=(-20, 100),
                subplot=True, alpha=0.3, xaxis=False, yaxis=False)



    def update(attr,old,new):
        notification_div.text = thistab.notification_updater('calculations ongoing')
        thistab.load_data()
        thistab.label_churned_retained()
        stream_launch_plots.event(launch_plots=True)
        notification_div.text = thistab.notification_updater("")
    try:
        thistab = Thistab()
        notification_text = thistab.notification_updater("Hello")
        notification_div = Div(text=notification_text, width=500, height=50)


        stream_launch_plots = streams.Stream.define('Start_date',
                                                    launch_plots=True)()

        thistab.make_checkboxes()
        launch_button = thistab.make_modelling_button()

        hv_difficulty = hv.DynamicMap(thistab.difficulty_plot,
                                    streams=[stream_launch_plots],
                                    datashade=True)


        renderer = hv.renderer('bokeh')
        difficulty = renderer.get_plot(hv_difficulty)

        # handle callbacks
        launch_button.on_click(update)

        controls = WidgetBox(
            thistab.checkbox_group,launch_button)

        grid = gridplot([[controls],
                         [notification_div],
                         [difficulty.state]
                        ])

        tab = Panel(child=grid, title='Tier '+str(tier)+' pre churn model ')
        return tab


    except Exception:
        logger.error('rendering err:',exc_info=True)
        return tab_error_flag('tier1_churned_model')




