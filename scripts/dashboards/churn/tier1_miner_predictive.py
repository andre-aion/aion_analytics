from scripts.utils.mylogger import mylogger
from scripts.utils.modeling.churn.miner_predictive_tab1 import MinerChurnedPredictiveTab1
from scripts.utils.myutils import tab_error_flag

from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel
from bokeh.models.widgets import Div, \
    DatePicker, Button

from datetime import datetime
from holoviews import streams
import holoviews as hv
from tornado.gen import coroutine
from config.df_construct_config import load_columns


lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)

@coroutine
def tier1_miner_churn_predictive_tab():
    class Thistab(MinerChurnedPredictiveTab1):
        def __init__(self,tier,cols):
            MinerChurnedPredictiveTab1.__init__(self, tier, cols=cols)
            self.cols = cols
            self.table = 'block_tx_warehouse'

            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                             <h1 style="color:#fff;">
                                                                             {}</h1></div>""".format('Welcome')
            self.notification_div = Div(text=txt, width=1400, height=20)

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                             <h4 style="color:#fff;">
                                                                             {}</h4></div>""".format(text)
            self.notification_div.text = txt

    def update_model():
        thistab.notification_updater('data reload,hyp testing ongoing')
        thistab.load_data()
        thistab.load_data_flag = False
        stream_update_reference_data.event(launch=True)
        stream_select_variable.event(variable=thistab.select_variable.value)
        thistab.notification_updater("ready")

    def update_plots(attr,old,new):
        thistab.notification_updater('updating plot(s) calculations ongoing')
        stream_select_variable.event(variable=thistab.select_variable.value)
        thistab.notification_updater("ready")

    def update_prediction():
        thistab.notification_updater('prediction calculations ongoing')
        thistab.start_date = datepicker_start.value
        thistab.end_date = datepicker_end.value
        stream_launch_prediction.event(launch=True)
        thistab.notification_updater("ready")

    def update_start_date(attr,old,new):
        thistab.notification_updater('updating start date')
        thistab.start_date = datepicker_start.value
        thistab.notification_updater("ready")

    def update_end_date(attr,old,new):
        thistab.notification_updater('updating end date')
        thistab.end_date = datepicker_end.value
        thistab.notification_updater("ready")

    try:
        # SETUP
        thistab = Thistab(tier=1,cols=load_columns['block_tx_warehouse']['churn'])
        thistab.make_checkboxes()
        thistab.load_data()

        # dates
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        first_date = datetime.strptime("2018-12-15 00:00:00", '%Y-%m-%d %H:%M:%S')
        #last_date = datetime.now().date()
        last_date = datetime.strptime("2018-12-30 00:00:00", '%Y-%m-%d %H:%M:%S')

        stream_update_reference_data = streams.Stream.define('Launch',launch=True)()
        stream_select_variable = streams.Stream.define('Select_variable',
                                                    variable='approx_value')()
        stream_launch_prediction = streams.Stream.define('Launch_predictions',
                                                         launch=True)()

        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Prediction period start date", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="Prediction period end date", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        refresh_checkbox_button = thistab.make_button('Refresh checkboxes')

        thistab.select_variable = thistab.make_selector('Choose variable','approx_value')
        update_data_button = thistab.make_button('Update data')
        launch_predict_button = Button(label='Make predictions for ...',button_type="success")


        # PLOTS
        hv_plot1 = hv.DynamicMap(thistab.box_plot,
                                 streams=[stream_select_variable,
                                          stream_update_reference_data])

        hv_hypothesis_table = hv.DynamicMap(thistab.hypothesis_table,
                                            streams=[stream_update_reference_data])
        hv_prediction_table = hv.DynamicMap(thistab.prediction_table,
                                            streams=[stream_launch_prediction])

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
        datepicker_start.on_change('value', update_start_date)
        datepicker_end.on_change('value',update_end_date)

        # organize layout
        model_controls = WidgetBox(thistab.checkbox_group,
                                   refresh_checkbox_button,
                                   thistab.select_variable,
                                   update_data_button)

        predict_controls = WidgetBox(
            launch_predict_button,
            datepicker_start,
            datepicker_end
            )

        grid = gridplot([[thistab.notification_div],
                         [model_controls, thistab.spacing_div, thistab.desc_load_data_div, thistab.desc_hypothesis_div],
                         [plot.state, hypothesis_table.state],
                         [predict_controls, thistab.desc_prediction_div],
                         [prediction_table.state, thistab.metrics_div]
                        ])

        tab = Panel(child=grid, title='Tier '+str(1)+' churn predictions ')
        return tab


    except Exception:
        logger.error('rendering err:',exc_info=True)
        text = 'Tier '+str(1)+'miner_predictive_model'
        return tab_error_flag(text)




