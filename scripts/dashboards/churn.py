from os.path import join, dirname

from scripts.utils.mylogger import mylogger
from scripts.utils.poolminer import make_tier1_list, \
    make_tier2_list, is_tier2_in_memory, is_tier1_in_memory
from scripts.utils.myutils import tab_error_flag
from scripts.utils.mytab import Mytab
from config import dedup_cols
from tornado import gen
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel
import gc
from bokeh.models.widgets import  Div, \
    DatePicker, TableColumn, DataTable, Button, Select
from holoviews import streams
from pdb import set_trace
import hvplot.dask
import hvplot.pandas

import holoviews as hv, param, dask.dataframe as dd
from holoviews.operation.datashader import rasterize, shade, datashade
from datetime import datetime
import numpy as np
import pandas as pd
import dask as dd
from pdb import set_trace
from holoviews import streams

import holoviews as hv
import time
from tornado.gen import coroutine

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
tables = {}
tables['block'] = 'block'
tables['transaction'] = 'transaction'

menu = [str(x * 0.5) for x in range(0, 80)]
menu_blocks_mined = [str(x) if x > 0 else '' for x in range(0, 50)]

@coroutine
def churn_tab():
    # source for top N table
    tier2_src = ColumnDataSource(data=dict(
        to_addr=[],
        block_timestamp=[],
        approx_value=[]))

    tier1_src = ColumnDataSource(data=dict(
        from_addr=[],
        block_timestamp=[],
        block_number=[],
        approx_value=[]))

    block_cols = ['transaction_hashes', 'block_timestamp', 'miner_address', 'block_number']
    transaction_cols = ['block_timestamp',
                        'transaction_hash', 'from_addr',
                        'to_addr', 'approx_value']
    warehouse_cols = ['block_timestamp', 'block_number', 'to_addr',
                      'from_addr', 'miner_address', 'approx_value', 'transaction_hash']


    class Thistab():

        def __init__(self,key_tab):
            self.tier1_miners_activated = {
                'reference': False,
                'period': False
            }
            self.tab = {}
            self.tab['reference'] = Mytab('block_tx_warehouse',warehouse_cols, dedup_cols)
            self.tab['period'] = Mytab('block_tx_warehouse',warehouse_cols, dedup_cols)
            self.tab['period'].key_tab = key_tab
            self.tab['reference'].key_tab = key_tab

            self.tab['reference'].construction_tables['block'] = Mytab('block',block_cols, dedup_cols)
            self.tab['reference'].construction_tables['block'].key_tab = 'poolminer'
            self.tab['reference'].construction_tables['transaction'] = Mytab('transaction',transaction_cols, dedup_cols)
            self.tab['reference'].construction_tables['transaction'].key_tab = 'poolminer'

            self.tab['period'].construction_tables['block'] = Mytab('block', block_cols, dedup_cols)
            self.tab['period'].construction_tables['block'].key_tab = 'poolminer'
            self.tab['period'].construction_tables['transaction'] = Mytab('transaction', transaction_cols, dedup_cols)
            self.tab['period'].construction_tables['transaction'].key_tab = 'poolminer'

            self.threshold_tx_received = .5
            self.threshold_tx_paid_out = 1
            self.threshold_blocks_mined = 1


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
                        tier1_miners_list = make_tier1_list(self.tab[when].df1, start_date,
                                                            end_date,
                                                            self.threshold_tx_paid_out,
                                                            self.threshold_blocks_mined)
                        self.tab[when].df_load(start_date, end_date)
                    else:
                        tier1_miners_list = self.tab[when].tier1_miners_list


                    tier2_miners_list = \
                        make_tier2_list(self.tab[when].df1, start_date, end_date,
                                        tier1_miners_list,
                                        threshold_tier2_received=self.threshold_tx_received,
                                        threshold_tx_paid_out=self.threshold_tx_paid_out,
                                        threshold_blocks_mined_per_day=self.threshold_blocks_mined)

                    self.tab[when].tier1_miners_list = tier2_miners_list
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
                return self.stats('TIER 1',tier1_ref_miners_list,tier1_period_miners_list)
            except Exception:
                logger.error("tier 1 churn",exc_info=True)


        def stats(self,tier,ref_list, period_list):
            try:
                # STATS OF INTEREST
                # percentage churned, churn count

                churn_count = len(list(set(ref_list).difference(period_list)))
                if len(ref_list) == 0:
                    churned_percentage = 0
                    logger.warning("there are no reference tier1 miners. denom = 0")
                else:
                    churned_percentage = 100 * churn_count / len(ref_list)

                # new miners
                new_miners_count =len(list(set(period_list).difference(ref_list)))
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
                        """.format(tier,
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
                logger.error("tier 2 churn", exc_info=True)


        def tier2_churn(self, period_start_date, period_end_date,
                        ref_start_date, ref_end_date):
            logger.warning("tier 2 churn started")

            try:
                tier2_period_miners_list = self.make_tier2_miners_list('period',period_start_date,
                                                                       period_end_date)

                tier2_reference_miners_list = self.make_tier2_miners_list('reference', ref_start_date,
                                                                          ref_end_date)

                logger.warning("tier 2 churn completed")

                return self.stats('TIER 2',tier2_reference_miners_list,tier2_period_miners_list)

            except Exception:
                logger.error("tier 2 churn", exc_info=True)



        def notification_updater(self, text):
            return '<h3  style="color:red">{}</h3>'.format(text)


    def update_threshold_tier_2_received(attrname, old, new):
        notification_div.text = thistab.notification_updater \
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
        notification_div.text = thistab.notification_updater("")

    def update(attr, old, new):
        notification_div.text = thistab.notification_updater \
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
        notification_div.text = thistab.notification_updater("")


    try:
        thistab = Thistab('poolminer')

        # STATIC DATES
        # format dates
        first_date_range = datetime.strptime("2018-04-23 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()

        ref_first_date = datetime.strptime("2018-12-01 00:00:00",'%Y-%m-%d %H:%M:%S')
        ref_last_date = datetime.strptime("2018-12-15 00:00:00",'%Y-%m-%d %H:%M:%S')
        period_first_date = datetime.strptime("2018-12-15 00:00:00",'%Y-%m-%d %H:%M:%S')
        period_last_date = last_date_range

        tier1_text = thistab.tier1_churn(period_first_date, period_last_date,
                                         ref_first_date, ref_last_date)

        tier2_text = thistab.tier2_churn(period_first_date, period_last_date,
                                         ref_first_date, ref_last_date)

        notification_text = thistab.notification_updater("")


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
        # Notification
        notification_div = Div(text=notification_text,width=500,height=50)


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

        # create the dashboard
        grid = gridplot([[controls_left,controls_right],
                         [notification_div],
                [tier1_stats,tier2_stats]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Churn')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag('churn')