from os.path import join, dirname

from scripts.utils.mylogger import mylogger
from scripts.utils.poolminer import make_tier1_list, \
    make_tier2_list, is_tier2_in_memory, is_tier1_in_memory
from scripts.utils.myutils import tab_error_flag
from scripts.utils.mytab import Mytab, DataLocation
from config import dedup_cols, columns as cols
from tornado import gen
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d, CustomJS
import gc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider, Div, \
    DatePicker, TableColumn, DataTable, Button, Select, Paragraph
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
        block_date=[],
        approx_value=[]))

    tier1_src = ColumnDataSource(data=dict(
        from_addr=[],
        block_date=[],
        block_number=[],
        approx_value=[]))

    class Thistab(Mytab):
        cols = ['transaction_hashes','block_date','block_timestamp','miner_address','block_number']
        block_tab = Mytab('block', cols, dedup_cols)
        block_tab.key_tab = 'poolminer'
        ref_block_tab = Mytab('block', cols, dedup_cols)
        ref_block_tab.key_tab = 'poolminer'

        cols = ['block_date','block_timestamp',
                'transaction_hash', 'from_addr',
                'to_addr', 'approx_value']
        transaction_tab = Mytab('transaction', cols, dedup_cols)
        transaction_tab.key_tab = 'poolminer'
        ref_transaction_tab = Mytab('transaction', cols, dedup_cols)
        ref_transaction_tab.key_tab = 'poolminer'

        ref_block_tx_warehouse_tab = Mytab('block_tx_warehouse', cols, [])
        tier1_ref_miners_activated = False
        tier1_period_miners_activated = False

        def __init__(self, table,key_tab='',cols=[], dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.tier1_df = self.df
            self.tier2_df = self.df
            self.threshold_tier2_received = .5
            self.threshold_tx_paid_out = 1
            self.threshold_blocks_mined = 1
            self.tier2_miners_list = []
            self.tier1_miners_list = []
            self.key_tab = key_tab

            self.tier1_ref_miners_list = []
            self.tier1_period_miners_list = []

        def df_loaded_check(self, start_date, end_date):
            # check to see if block_tx_warehouse is loaded
            data_location = self.is_data_in_memory(start_date, end_date)
            if data_location == DataLocation.IN_MEMORY:
                # logger.warning('warehouse already loaded:%s', self.df.tail(40))
                pass
            elif data_location == DataLocation.IN_REDIS:
                self.load_data(start_date, end_date)
            else:
                # load the two tables and make the block
                self.block_tab.load_data(start_date, end_date)
                self.transaction_tab.load_data(start_date, end_date)
                self.load_data(start_date, end_date, df_tx=self.transaction_tab.df,
                               df_block=self.block_tab.df)
            self.filter_df(start_date,end_date)

        def make_tier1_miners(self, start_date, end_date):
            # TIER 1 MINERS
            tier_1_miners_list = is_tier1_in_memory(start_date, end_date,
                                                    self.threshold_tx_paid_out,
                                                    self.threshold_blocks_mined)
            if self.df is None or len(self.df) <= 0:
                self.df_loaded_check(start_date,end_date)
                self.filter_df(start_date, end_date)


            # generate the list if necessary
            if tier_1_miners_list is None:
                values = {'approx_value': 0, 'to_addr': 'unknown',
                          'from_addr': 'unknown', 'block_number': 0}
                self.df1 = self.df1.fillna(values)
                # with data in hand, make the list
                tier_1_miners_list = make_tier1_list(self.df1, start_date, end_date,
                                                     self.threshold_tx_paid_out,
                                                     self.threshold_blocks_mined)
            return tier_1_miners_list

        def ref_warehouse_loaded_check(self, start_date, end_date):
            # check to see if block_tx_warehouse is loaded
            data_location = self.ref_block_tx_warehouse_tab.is_data_in_memory(start_date, end_date)
            if data_location == DataLocation.IN_MEMORY:
                # logger.warning('warehouse already loaded:%s', self.df.tail(40))
                pass
            elif data_location == DataLocation.IN_REDIS:
                self.ref_block_tx_warehouse_tab.load_data(start_date, end_date)
            else:
                # load the two tables and make the block
                self.ref_block_tab.load_data(start_date, end_date)
                self.ref_transaction_tab.load_data(start_date, end_date)
                self.ref_block_tx_warehouse_tab.load_data(start_date, end_date,
                                                          df_tx=self.ref_transaction_tab.df,
                                                          df_block=self.ref_block_tab.df)

            self.ref_block_tx_warehouse_tab.filter_df(start_date,end_date)


        def make_ref_tier_1_miners(self, start_date, end_date):
            # TIER 1 MINERS
            tier_1_miners_list = is_tier1_in_memory(start_date, end_date,
                                                    self.threshold_tx_paid_out,
                                                    self.threshold_blocks_mined)

            # generate the list if necessary
            if tier_1_miners_list is None:
                if self.ref_block_tx_warehouse_tab.df is None or len(self.ref_block_tx_warehouse_tab.df) <= 0:
                    self.ref_warehouse_loaded_check(start_date, end_date)
                values = {'approx_value': 0, 'to_addr': 'unknown',
                          'from_addr': 'unknown', 'block_number': 0}
                self.ref_block_tx_warehouse_tab.df1 = self.ref_block_tx_warehouse_tab.df1.fillna(values)
                # with data in hand, make the list
                tier_1_miners_list = make_tier1_list(self.ref_block_tx_warehouse_tab.df1, start_date, end_date,
                                                     self.threshold_tx_paid_out,
                                                     self.threshold_blocks_mined)
            return tier_1_miners_list

        def make_tier1_miners_list(self, when, start_date, end_date, threshold_tx_paid_out,
                                   threshold_blocks_mined):
            logger.warning("tier 1 triggered:%s")

            if isinstance(threshold_blocks_mined, str):
                threshold_blocks_mined = int(threshold_blocks_mined)
            if isinstance(threshold_tx_paid_out, str):
                threshold_tx_paid_out = float(threshold_tx_paid_out)

            self.threshold_tx_paid_out = threshold_tx_paid_out
            self.threshold_blocks_mined = threshold_blocks_mined
            end_date = datetime.combine(end_date, datetime.min.time())
            start_date = datetime.combine(start_date, datetime.min.time())

            if when == 'reference':
                self.tier1_ref_miners_list = self.make_ref_tier_1_miners(start_date, end_date)
            else:
                # ensure tier1 is loaded
                self.tier1_period_miners_list = self.make_tier1_miners(start_date, end_date)


        def make_tier2_miners_list(self,when, start_date, end_date,
                                   threshold_tx_received,
                                   threshold_tx_paid_out,
                                   threshold_blocks_mined):
            logger.warning("tier 2 triggered:%s", threshold_tx_received)
            if isinstance(threshold_tx_received, str):
                threshold_tx_received = float(threshold_tx_received)
            if isinstance(threshold_blocks_mined, str):
                threshold_blocks_mined = int(threshold_blocks_mined)
            if isinstance(threshold_tx_paid_out, str):
                threshold_tx_paid_out = float(threshold_tx_paid_out)
            self.threshold_tx_paid_out = threshold_tx_paid_out
            self.threshold_blocks_mined = threshold_blocks_mined
            self.threshold_tier2_received = threshold_tx_received

            tier2_miners_list = is_tier2_in_memory(start_date, end_date,
                                                   threshold_tx_paid_out,
                                                   threshold_blocks_mined)
            # generate the list if necessary
            if tier2_miners_list is None:
                # get tier 1 miners list
                if when == 'reference':
                    # if both tier1 and tier2 are being run, then skip the calculationf for the tier1
                    # miners list and the warehouse. they just been calculated
                    if not self.tier1_ref_miners_activated:
                        self.make_tier1_miners_list('reference',start_date, end_date,
                                                                         threshold_tx_paid_out,
                                                                         threshold_blocks_mined)
                        self.ref_warehouse_loaded_check(start_date, end_date)

                    tier2_miners_list = \
                        make_tier2_list(self.ref_block_tx_warehouse_tab.df1, start_date, end_date,
                                        self.tier1_ref_miners_list,
                                        threshold_tier2_received=threshold_tx_received,
                                        threshold_tx_paid_out=threshold_tx_paid_out,
                                        threshold_blocks_mined_per_day=threshold_blocks_mined)

                    self.tier1_ref_miners_activated = False

                else:
                    if not self.tier1_period_miners_activated:
                        self.make_tier1_miners_list('period',start_date, end_date,
                                                                         threshold_tx_paid_out,
                                                                         threshold_blocks_mined)
                        self.df_loaded_check(start_date, end_date)
                    tier2_miners_list = \
                        make_tier2_list(self.df1, start_date, end_date,
                                        self.tier1_period_miners_list,
                                        threshold_tier2_received=threshold_tx_received,
                                        threshold_tx_paid_out=threshold_tx_paid_out,
                                        threshold_blocks_mined_per_day=threshold_blocks_mined)

                    self.tier1_period_miners_activated = False

            return tier2_miners_list

        # reference refers to past, period is the time under review for churn
        def tier1_churn(self, period_start_date, period_end_date,
                        ref_start_date, ref_end_date,
                        threshold_tx_paid_out, threshold_blocks_mined):
            # filter current data warehouse by the tier 1 miner list
            self.make_tier1_miners_list('reference',
                                        ref_start_date, ref_end_date,
                                        threshold_tx_paid_out,
                                        threshold_blocks_mined)

            self.make_tier1_miners_list('period',
                                       period_start_date, period_end_date,
                                       threshold_tx_paid_out,
                                       threshold_blocks_mined)

            logger.warning("tier 1 churn completed")
            return self.stats('TIER 1',self.tier1_ref_miners_list,self.tier1_period_miners_list)

        def stats(self,tier,ref_list, period_list):
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
                churned_percentage = 0
                logger.warning("there are no reference churn period miners. denom = 0")
            else:
                new_miners_percentage = new_miners_count * 100 / len(period_list)

            # display the data
            text = """ <h3>{} STATS:</h3> <br /> 
                      Total miners in reference period: {} <br /> 
                      Total miners in churn review period: {}  <br /> 
                      Number churned: {} <br /> 
                      Percentage churned: {}% <br /> 
                      New miners:{} <br /> 
                      Percentage new miners:{}%
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

        def tier2_churn(self, period_start_date, period_end_date,
                        ref_start_date, ref_end_date,
                        threshold_tx_received,
                        threshold_tx_paid_out, threshold_blocks_mined):
            period_tier2_miners_list = self.make_tier2_miners_list('period',period_start_date, period_end_date,
                                                                  threshold_tx_received,
                                                                  threshold_tx_paid_out,
                                                                  threshold_blocks_mined)
            ref_tier2_miners_list = self.make_tier2_miners_list('reference', ref_start_date, ref_end_date,
                                                                threshold_tx_received,
                                                                threshold_tx_paid_out,
                                                                threshold_blocks_mined)
            logger.warning("tier 2 churn completed")

            return self.stats('TIER 2',ref_tier2_miners_list,period_tier2_miners_list)

    def update_threshold_tier_2_received(attrname, old, new):
        notification_div.text = thistab.notification_updater\
            ("Tier 2 calculations in progress! Please wait.")
        tier2_stats.text = thistab.tier2_churn(datepicker_churn_start.value,
                            datepicker_churn_end.value,
                            datepicker_ref_start.value,
                            datepicker_ref_end.value,
                            select_tx_received.value,
                            select_tx_paid_out.value,
                            select_blocks_mined.value)
        notification_div.text = thistab.notification_updater("")

    def update(attr, old, new):
        notification_div.text = thistab.notification_updater \
            ("Tiers 1 and 2 calculations in progress! Please wait.")
        thistab.tier1_ref_miners_activated = True
        thistab.tier1_period_miners_activated = True
        tier1_stats.text = thistab.tier1_churn(datepicker_churn_start.value,
                            datepicker_churn_end.value,
                            datepicker_ref_start.value,
                            datepicker_ref_end.value,
                            select_tx_paid_out.value,
                            select_blocks_mined.value)

        tier2_stats.text = thistab.tier2_churn(datepicker_churn_start.value,
                            datepicker_churn_end.value,
                            datepicker_ref_start.value,
                            datepicker_ref_end.value,
                            select_tx_received.value,
                            select_tx_paid_out.value,
                            select_blocks_mined.value)
        thistab.tier1_ref_miners_activated = True
        thistab.tier1_period_miners_activated = True
        notification_div.text = thistab.notification_updater("")


    try:
        cols = ['block_date','block_timestamp', 'block_number', 'to_addr',
                      'from_addr', 'miner_address', 'approx_value', 'transaction_hash']
        thistab = Thistab('block_tx_warehouse', cols)

        # STATIC DATES
        # format dates
        first_date_range = datetime.strptime("2018-04-23 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()

        ref_first_date = datetime.strptime("2018-10-01","%Y-%m-%d")
        ref_last_date = datetime.strptime("2018-11-30","%Y-%m-%d")
        period_first_date = datetime.strptime("2018-11-01","%Y-%m-%d")
        period_last_date = last_date_range


        tier1_text = thistab.tier1_churn(period_first_date, period_last_date,
                                          ref_first_date, ref_last_date,
                                          thistab.threshold_tx_paid_out,
                                          thistab.threshold_blocks_mined)

        tier2_text = thistab.tier2_churn(period_first_date, period_last_date,
                                          ref_first_date, ref_last_date,
                                          thistab.threshold_tier2_received,
                                          thistab.threshold_tx_paid_out,
                                          thistab.threshold_blocks_mined)

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
                                    value=str(thistab.threshold_tier2_received),
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