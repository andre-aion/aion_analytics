from scripts.utils.mylogger import mylogger
from scripts.utils.dashboard.churned_model_tab import ChurnedModelTab
from scripts.utils.myutils import tab_error_flag

from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel
from bokeh.models.widgets import  Div, \
    DatePicker

from datetime import datetime
from holoviews import streams
import holoviews as hv
from tornado.gen import coroutine
from data.config import load_columns


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

    def update_model():
        thistab.notification_updater('please wait, calculations ongoing')
        thistab.load_data()
        thistab.load_data_flag = False
        stream_update_data.event(launch=True)
        stream_select_variable.event(variable=thistab.select_variable.value)
        thistab.notification_updater("")

    def update_plots(attr,old,new):
        thistab.notification_updater('updating plot(s) calculations ongoing')
        stream_select_variable.event(variable=thistab.select_variable.value)
        thistab.notification_updater("")

    def update_prediction():
        thistab.notification_updater('prediction calculations ongoing')
        thistab.start_date = datepicker_start.value
        thistab.end_date = datepicker_end.value
        stream_launch_prediction.event(launch=True)
        thistab.notification_updater("")


    try:
        # SETUP
        thistab = Thistab(cols=load_columns['block_tx_warehouse']['churn'])
        thistab.notification_updater("")
        thistab.notification_div = Div(text='Welcome, lets do some machine learning', width=400, height=50)
        thistab.make_checkboxes()
        thistab.load_data()
        #thistab.make_predictions(datepicker_start.value,datepicker_end.value)

        # dates
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        first_date = datetime.strptime("2018-12-15 00:00:00", '%Y-%m-%d %H:%M:%S')
        #last_date = datetime.now().date()
        last_date = datetime.strptime("2018-12-30 00:00:00", '%Y-%m-%d %H:%M:%S')

        stream_update_data = streams.Stream.define('Launch',launch=True)()
        stream_select_variable = streams.Stream.define('Select_variable',
                                                    variable='approx_value')()
        stream_launch_prediction = streams.Stream.define('Launch _predictions',
                                                         launch=True)()

        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        #launch_button = thistab.make_button('Launch model')
        refresh_checkbox_button = thistab.make_button('Refresh checkboxes')

        thistab.select_variable = thistab.make_selector('Choose variable','approx_value')
        update_data_button = thistab.make_button('Update data')
        launch_predict_button = thistab.make_button('Make predictions')


        # PLOTS
        hv_plot1 = hv.DynamicMap(thistab.box_plot,
                                 streams=[stream_select_variable,
                                          stream_update_data])

        #hypothesis_div = thistab.results_div(text=hyp_text)
        hv_hypothesis_table = hv.DynamicMap(thistab.hypothesis_table,
                                            streams=[stream_update_data])
        hv_prediction_table = hv.DynamicMap(thistab.prediction_table,
                                          streams=[stream_launch_prediction])\

        renderer = hv.renderer('bokeh')
        plot = renderer.get_plot(hv_plot1)
        hypothesis_table = renderer.get_plot(hv_hypothesis_table)
        prediction_table = renderer.get_plot(hv_prediction_table)

        # handle callbacks
        update_data_button.on_click(update_model)
        refresh_checkbox_button.on_click(thistab.update_checkboxes)
        thistab.select_variable.on_change('value', update_plots)
        launch_predict_button.on_click(update_prediction)
        thistab.checkbox_group.on_change('active',thistab.set_load_data_flag)

        # organize layout
        model_controls = WidgetBox(thistab.checkbox_group,
                             refresh_checkbox_button,
                             thistab.select_variable,
                             update_data_button)

        predict_controls = WidgetBox(
            datepicker_start,
            datepicker_end,
            launch_predict_button)

        grid = gridplot([[model_controls,thistab.notification_div, ],
                         [plot.state, hypothesis_table.state],
                         [predict_controls],
                         [prediction_table.state,thistab.metrics_div]
                        ])

        tab = Panel(child=grid, title='Tier '+str(tier)+' pre churn model ')
        return tab


    except Exception:
        logger.error('rendering err:',exc_info=True)
        return tab_error_flag('tier1_churned_model')




