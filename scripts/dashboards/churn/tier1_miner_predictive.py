from scripts.utils.modeling.churn.miner_predictive_methods import find_in_redis
from scripts.utils.mylogger import mylogger
from scripts.utils.modeling.churn.miner_predictive_tab import MinerChurnedPredictiveTab
from scripts.utils.myutils import tab_error_flag

from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel
from bokeh.models.widgets import Div, \
    DatePicker, Button, CheckboxGroup, Select

from datetime import datetime
from holoviews import streams
import holoviews as hv
from tornado.gen import coroutine
from config.df_construct_config import load_columns
import dask.dataframe as dd


lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)

@coroutine
def tier1_miner_churn_predictive_tab():
    class Thistab(MinerChurnedPredictiveTab):
        def __init__(self,tier,cols):
            MinerChurnedPredictiveTab.__init__(self, tier, cols=cols)
            self.cols = cols
            self.table = 'block_tx_warehouse'

            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                             <h1 style="color:#fff;">
                                                                             {}</h1></div>""".format('Welcome')
            self.notification_div = Div(text=txt, width=1400, height=20)
            self.metrics_div = Div(text='')
            # BOKEH MODELS
            text = """
                            <div {}>
                            <h3 {}>Checkboxlist Info:</h3>Use the checkbox 
                            list to the left to select <br/>
                            the reference period and parameters <br/>
                            for building the predictive model.<br/>
                            1) Select the desired parameter(s).<br/>
                            2) Click "update data" to update only the <br/>
                               graphs and impact variables.<br/>
                            3) Choose the prediction date range.<br/>
                            4) Click "Make predictions"
                            </div>
                            """.format(self.div_style, self.header_style)
            self.desc_load_data_div = Div(text=text, width=300, height=300)
            # hypothesis
            text = """
                    <div {}>
                    <h3 {}>Hypothesis test info:</h3>
                    <ul>
                    <li>
                    The table below shows which variables 
                    do/do not affect churn.
                    </li>
                    <li>
                    The ones that do not can be ignored.
                    </li>
                    <li> 
                    The figure (below left) shows the difference 
                    in behavior between those who left <br/>
                    vs those who remained.<br/>
                    </li>
                    <li>
                    Select the variable from the dropdown <br/>
                    list to change the graph.
                    </li></ul>
                    </div>
                    """.format(self.div_style, self.header_style)
            self.desc_hypothesis_div = Div(text=text, width=300, height=300)

            # prediction
            text = """
                    <div {}>
                    <h3 {}>Prediction Info:</h3>
                    <ul><li>
                    The table below shows the miners <br/>
                    operating in the selected period,<br/>
                    and whether they are likely to churn.<br/>
                    <li>
                    Use the datepicker(s) to the left to select the period you wish to predict.
                    </li></ul>
                    </div> 
                    """.format(self.div_style, self.header_style)
            self.desc_prediction_div = Div(text=text, width=350, height=100)

            # spacing div
            self.spacing_div = Div(text='', width=50, height=200)

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                             <h4 style="color:#fff;">
                                                                             {}</h4></div>""".format(text)
            self.notification_div.text = txt


    def make_button(self,label):
        try:
            # make list of
            button = Button(label=label, button_type="success")
            return button
        except Exception:
            logger.error('make modeling button', exc_info=True)

    def make_selector(self,title,initial_value):
        try:
            selector = Select(title=title,
                              value=initial_value,
                              options=self.hyp_variables)
            logger.warning("%s SELECTOR CREATED",initial_value.upper())

            return selector
        except Exception:
            logger.error('make selector', exc_info=True)


    def results_div(self, text, width=600, height=300):
        div = Div(text=text, width=width, height=height)
        return div

    def title_div(self, text, width=700):
        text = '<h2 style="color:green;">{}</h2>'.format(text)
        return Div(text=text, width=width, height=15)

        # show checkbox list of reference periods produced by the churn tab

    def make_checkboxes(self):
        try:
            # make list of
            active = 1
            self.checkbox_group = CheckboxGroup(labels=[],
                                                active=[active])
            self.update_checkboxes()
        except Exception:
            logger.error('make checkboxes', exc_info=True)

    def update_checkboxes(self):
        try:
            if self.tier in [1, "1"]:
                item = "tier1_churned_dict"
            else:
                item = "tier2_churned_dict"
            lst = find_in_redis(item)
            self.checkbox_group.labels = lst
            logger.warning("CHECKBOX LIST:%s", lst)
        except Exception:
            logger.error('update checkboxes', exc_info=True)

        # PLOTS
        def box_plot(self, variable='approx_value', launch=False):
            try:
                # logger.warning("difficulty:%s", self.df.tail(30))
                # get max value of variable and multiply it by 1.1
                min, max = dd.compute(self.df_grouped[variable].min(),
                                      self.df_grouped[variable].max())
                return self.df_grouped.hvplot.box(variable, by='churned_verbose',
                                                  ylim=(.9 * min, 1.1 * max))
            except Exception:
                logger.error("box plot:", exc_info=True)

        def bar_plot(self, variable='approx_value', launch=False):
            try:
                # logger.warning("difficulty:%s", self.df.tail(30))
                # get max value of variable and multiply it by 1.1
                return self.df.hvplot.bar('miner_address', variable, rot=90,
                                          height=400, width=300, title='block_number by miner address',
                                          hover_cols=['percentage'])
            except Exception:
                logger.error("box plot:", exc_info=True)

        def hist(self, variable='approx_value'):
            try:
                # logger.warning("difficulty:%s", self.df.tail(30))
                # get max value of variable and multiply it by 1.1
                # min, max = dd.compute(self.df_grouped[variable].min(),
                # self.df_grouped[variable].max())
                return self.df_grouped.hvplot.hist(
                    y=variable, bins=50, by='churned', alpha=0.3)
            except Exception:
                logger.error("box plot:", exc_info=True)

    def prediction_table(self,launch=False):
        try:
            logger.warning("LOAD DATA FLAG in prediction table:%s",self.load_data_flag)
            self.make_predictions()
            return self.predict_df.hvplot.table(columns=['address', 'likely...'],
                                width=600,height=1200)
        except Exception:
            logger.error("prediction table:", exc_info=True)


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




