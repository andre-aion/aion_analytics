from scripts.utils.mylogger import mylogger
from scripts.utils.dashboards.poolminer import make_tier1_list, \
    make_tier2_list, is_tier2_in_memory, is_tier1_in_memory
from scripts.utils.myutils import tab_error_flag, datetime_to_date
from scripts.utils.dashboards.mytab_churn import MytabChurn
from scripts.storage.pythonRedis import PythonRedis
from config.df_construct_config import dedup_cols, load_columns as cols
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel
from bokeh.models.widgets import  Div, \
    DatePicker, Select

from datetime import datetime, timedelta

import holoviews as hv
from tornado.gen import coroutine
import os

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
tables = {}
tables['block'] = 'block'
tables['transaction'] = 'transaction'

menu = [str(x * 0.5) for x in range(0, 80)]
menu_blocks_mined = [str(x) if x > 0 else '' for x in range(0, 50)]
redis = PythonRedis()

# delete logs on load
list(map( os.unlink, (os.path.join('logs',f) for f in os.listdir('logs'))))


@coroutine
def churn_tab():


    class Thistab():

        def __init__(self,key_tab):
            self.tier1_miners_activated = {
                'reference': False,
                'period': False
            }
            self.tab = {}
            self.tab['reference'] = MytabChurn('block_tx_warehouse', cols['block_tx_warehouse'][key_tab], dedup_cols)
            self.tab['period'] = MytabChurn('block_tx_warehouse', cols['block_tx_warehouse'][key_tab], dedup_cols)
            self.tab['period'].key_tab = key_tab
            self.tab['reference'].key_tab = key_tab

            self.threshold_tx_received = .5
            self.threshold_tx_paid_out = 1
            self.threshold_blocks_mined = 1

            self.tab['reference'].start_date = ""
            self.tab['reference'].end_date = ""
            self.tab['period'].start_date = ""
            self.tab['period'].end_date = ""
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                      <h1 style="color:#fff;">
                                                                      {}</h1></div>""".format('Welcome')
            self.notification_div = Div(text=txt, width=1400, height=20)

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                      <h4 style="color:#fff;">
                                                                      {}</h4></div>""".format(text)
            self.notification_div.text = txt

        def make_tier1_miners_list(self, when, start_date, end_date):
            try:
                end_date = datetime.combine(end_date, datetime.min.time())
                start_date = datetime.combine(start_date, datetime.min.time())
                # TIER 1 MINERS
                tier1_miners_list = is_tier1_in_memory(start_date, end_date,
                                                        self.threshold_tx_paid_out,
                                                        self.threshold_blocks_mined)


                # generate the list if necessary
                values = {'approx_value': 0, 'to_addr': 'unknown',
                          'from_addr': 'unknown', 'block_number': 0}
                self.tab[when].df_load(start_date,end_date)
                self.tab[when].df1 = self.tab[when].df1.fillna(values)

                if tier1_miners_list is None:
                    # with data in hand, make the list
                    tier1_miners_list = make_tier1_list(self.tab[when].df1, start_date, end_date,
                                                        self.threshold_tx_paid_out,
                                                        self.threshold_blocks_mined)

                self.tab[when].tier1_miners_list = tier1_miners_list
                self.tab[when].start_date = start_date
                self.tab[when].end_date = end_date

                logger.warning("%s tier 1 miners list length:%s",when.upper(),
                               len(tier1_miners_list))

                return tier1_miners_list
            except Exception:
                logger.error("make tier 1 miners list", exc_info=True)

        def make_tier2_miners_list(self,when, start_date, end_date):
            try:

                tier2_miners_list = is_tier2_in_memory(start_date, end_date,
                                                       self.threshold_tx_received,
                                                       self.threshold_tx_paid_out,
                                                       self.threshold_blocks_mined)

                # generate the list if necessary
                if tier2_miners_list is None:
                    # get tier 1 miners list
                    activated = self.tier1_miners_activated[when]
                    if not activated:
                        self.tab[when].df_load(start_date, end_date)
                        tier1_miners_list = make_tier1_list(self.tab[when].df1, start_date,
                                                            end_date,
                                                            self.threshold_tx_paid_out,
                                                            self.threshold_blocks_mined)
                    else:
                        tier1_miners_list = self.tab[when].tier1_miners_list

                    tier2_miners_list = \
                        make_tier2_list(self.tab[when].df1, start_date, end_date,
                                        tier1_miners_list,
                                        threshold_tier2_received=self.threshold_tx_received,
                                        threshold_tx_paid_out=self.threshold_tx_paid_out,
                                        threshold_blocks_mined_per_day=self.threshold_blocks_mined)

                    self.tab[when].tier2_miners_list = tier2_miners_list
                    self.tier1_miners_activated[when] = False

                logger.warning("%s tier 2 miners list length:%s", when.upper(),
                               len(tier2_miners_list))

                return tier2_miners_list
            except Exception:
                logger.error("make tier 2 miner's list",exc_info=True)

        # reference refers to past, period is the time under review for churn
        def tier1_churn(self, period_start_date, period_end_date,
                        ref_start_date, ref_end_date):
            logger.warning("tier 1 churn started")

            try:
                # filter current data warehouse by the tier 1 miner list
                tier1_ref_miners_list = self.make_tier1_miners_list('reference',
                                            ref_start_date, ref_end_date)

                tier1_period_miners_list = self.make_tier1_miners_list('period',
                                           period_start_date, period_end_date)

                logger.warning("tier 1 churn completed")
                return self.stats(1,tier1_ref_miners_list,tier1_period_miners_list)
            except Exception:
                logger.error("tier 1 churn",exc_info=True)


        def stats(self,tier,ref_list, period_list):
            try:
                # STATS OF INTEREST
                # percentage churned, churn count
                if ref_list is None:
                    ref_list = []
                if period_list is None:
                    period_list = []
                churned_miners = list(set(ref_list).difference(period_list))
                new_miners = list(set(period_list).difference(ref_list))
                retained_miners = list(set(period_list).intersection(ref_list))

                # construct and save churned dictionary
                df_key_params = ['block_tx_warehouse', self.tab['reference'].key_tab]
                lst_key_params = [self.threshold_tx_received, self.threshold_tx_paid_out,
                                  self.threshold_blocks_mined]


                dct_name = 'tier'+str(tier)+'_churned_dict'
                churned_dict = {
                    'key_params': [dct_name, lst_key_params[1], lst_key_params[2]],
                    'reference_start_date': self.tab['reference'].start_date,
                    'reference_end_date': self.tab['reference'].end_date,
                    'period_start_date': self.tab['period'].start_date,
                    'period_end_date': self.tab['period'].end_date,
                    'warehouse': df_key_params,
                    'churned_lst': churned_miners,
                    'retained_lst': retained_miners
                }
                if tier in ['2',2]:
                    churned_dict['key_params'] = [dct_name,
                                                  lst_key_params[0],
                                                  lst_key_params[1],
                                                  lst_key_params[2]]
                redis.save_dict(churned_dict)

                # do the stats
                churn_count = len(churned_miners)


                if len(ref_list) == 0:
                    churned_percentage = 0
                    logger.warning("there are no reference tier1 miners. denom = 0")
                else:
                    churned_percentage = 100 * churn_count / len(ref_list)

                # new miners
                new_miners_count =len(new_miners)
                if len(period_list) == 0:
                    new_miners_percentage = 0
                    logger.warning("there are no reference churn period miners. denom = 0")
                else:
                    new_miners_percentage = new_miners_count * 100 / len(period_list)

                # display the data
                text = """ <h3>{} STATS:</h3> <br /> 
                          Total miners in reference period: {} <br /> 
                          Total miners in churn review period: {}  <br /> 
                          Number of miners departed: {} <br /> 
                          Percentage departed: {}% <br /> 
                          New miners:{} <br /> 
                          Percentage that are new miners:{}%
                        """.format("TIER "+str(tier),
                                   len(ref_list),
                                   len(period_list),
                                   churn_count,
                                   int(round(churned_percentage)),
                                   new_miners_count,
                                   int(round(new_miners_percentage))
                                   )
                # print notifications
                return text
            except Exception:
                logger.error("stats", exc_info=True)


        def tier2_churn(self, period_start_date, period_end_date,
                        ref_start_date, ref_end_date):
            logger.warning("tier 2 churn started")

            try:
                tier2_period_miners_list = self.make_tier2_miners_list('period',period_start_date,
                                                                       period_end_date)

                tier2_reference_miners_list = self.make_tier2_miners_list('reference', ref_start_date,
                                                                          ref_end_date)

                logger.warning("tier 2 churn completed")

                return self.stats(2,tier2_reference_miners_list,tier2_period_miners_list)

            except Exception:
                logger.error("tier 2 churn", exc_info=True)





    def update_threshold_tier_2_received(attrname, old, new):
        thistab.notification_updater \
            ("Tier 2 calculations in progress! Please wait.")

        if isinstance(select_tx_received.value, str):
            thistab.threshold_tx_received = float(select_tx_received.value)
        if isinstance(select_blocks_mined.value, str):
            thistab.threshold_blocks_mined = int(select_blocks_mined.value)
        if isinstance(select_tx_paid_out.value, str):
            thistab.threshold_tx_paid_out = float(select_tx_paid_out.value)

        tier2_stats.text = thistab.tier2_churn(datepicker_churn_start.value,
                                               datepicker_churn_end.value,
                                               datepicker_ref_start.value,
                                               datepicker_ref_end.value)
        thistab.notification_updater("ready")

    def update(attr, old, new):
        thistab.notification_updater \
            ("Tiers 1 and 2 calculations in progress! Please wait.")

        if isinstance(select_tx_received.value, str):
            thistab.threshold_tx_received = float(select_tx_received.value)
        if isinstance(select_blocks_mined.value, str):
            thistab.threshold_blocks_mined = int(select_blocks_mined.value)
        if isinstance(select_tx_paid_out.value, str):
            thistab.threshold_tx_paid_out = float(select_tx_paid_out.value)

        thistab.tier1_miners_activated['reference'] = True
        thistab.tier1_miners_activated['period'] = True
        tier1_stats.text = thistab.tier1_churn(datepicker_churn_start.value,
                                               datepicker_churn_end.value,
                                               datepicker_ref_start.value,
                                               datepicker_ref_end.value)

        tier2_stats.text = thistab.tier2_churn(datepicker_churn_start.value,
                                               datepicker_churn_end.value,
                                               datepicker_ref_start.value,
                                               datepicker_ref_end.value)
        thistab.tier1_miners_activated['reference'] = False
        thistab.tier1_miners_activated['period'] = False
        thistab.notification_updater("ready")


    try:
        thistab = Thistab('churn')

        # STATIC DATES
        # format dates
        first_date_range = datetime.strptime("2018-04-23 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()

        # calculate date load ranges
        range = 8
        period_last_date = datetime.now()
        period_first_date = datetime_to_date(period_last_date - timedelta(days=range))
        ref_last_date = datetime_to_date(period_last_date - timedelta(days=range+1))
        ref_first_date = datetime_to_date(period_last_date - timedelta(days=range*2))
        period_last_date = datetime_to_date(period_last_date)

        tier1_text = thistab.tier1_churn(period_first_date, period_last_date,
                                         ref_first_date, ref_last_date)

        tier2_text = thistab.tier2_churn(period_first_date, period_last_date,
                                         ref_first_date, ref_last_date)



        # MANAGE STREAM
        # date comes out stream in milliseconds

        # CREATE WIDGETS
        datepicker_churn_start = DatePicker(title="Churn period start", min_date=first_date_range,
                                      max_date=last_date_range, value=period_first_date)
        datepicker_churn_end = DatePicker(title="Churn period end", min_date=first_date_range,
                                     max_date=last_date_range, value=period_last_date)
        datepicker_ref_start = DatePicker(title="Reference period start", min_date=first_date_range,
                                      max_date=last_date_range, value=ref_first_date)
        datepicker_ref_end = DatePicker(title="Reference period end", min_date=first_date_range,
                                    max_date=last_date_range, value=ref_last_date)
        select_tx_received = Select(title='Threshold, Tier2: daily tx received',
                                    value=str(thistab.threshold_tx_received),
                                    options=menu)
        select_blocks_mined = Select(title='Threshold, Tier1: blocks mined',
                                     value=str(thistab.threshold_blocks_mined),
                                     options=menu_blocks_mined)
        select_tx_paid_out = Select(title='Threshold, Tier1: tx paid out',
                                    value=str(thistab.threshold_tx_paid_out),
                                    options=menu)

        tier1_stats = Div(text=tier1_text,width=300,height=400)
        tier2_stats = Div(text=tier2_text,width=300,height=400)


        # handle callbacks
        datepicker_churn_start.on_change('value', update)
        datepicker_churn_end.on_change('value', update)
        datepicker_ref_start.on_change('value', update)
        datepicker_ref_end.on_change('value', update)
        select_blocks_mined.on_change('value', update)
        select_tx_paid_out.on_change('value', update)

        select_tx_received.on_change("value", update_threshold_tier_2_received) # tier2 only callback


        # COMPOSE LAYOUT
        # put the controls in a single element
        controls_left = WidgetBox(
            datepicker_ref_start,
            datepicker_churn_start,
            select_tx_paid_out,
            select_blocks_mined)

        controls_right = WidgetBox(
            datepicker_ref_end,
            datepicker_churn_end,
            select_tx_received)

        # create the dashboards
        grid = gridplot([
            [thistab.notification_div],
            [controls_left,controls_right],
            [tier1_stats,tier2_stats]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Churn')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag('churn')