from datetime import datetime, timezone
import  time
from datetime import date as date
import pandas as pd
import numpy as np
import holoviews as hv
hv.extension('bokeh')

from bokeh.plotting import figure, output_file, show
from bokeh.models.widgets import DateRangeSlider, TextInput
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.transform import factor_cmap
from bokeh.palettes import Category20_16

from pdb import set_trace;
from scripts.utils.poolminer import munge_blockdetails

first_date = datetime(2018, 5, 1, 0, 0)
last_date = datetime.now()
first_date_str = first_date.strftime("Y-m-d")
last_date_str = last_date.strftime("Y-m-d")


def poolminer_tab(df):
    label = 'addr'
    TOOLS = "pan,wheel_zoom,box_zoom,reset,save,box_select,lasso_select"
    def prep_dataset(df_to_plot,start_date,end_date):
        # MUNGE DATA
        df_to_plot['block_timestamp'] = pd.to_datetime(df_to_plot['block_timestamp'],unit='s')

        # change from milliseconds to seconds
        if start_date > 1630763200:
            start_date = (start_date // 1000)
        if end_date > 1630763200:
            end_date = (end_date // 1000)

        # calculate percentage
        start_date = datetime.fromtimestamp(start_date)
        end_date = datetime.fromtimestamp(end_date)

        df_to_source = df_to_plot[(df_to_plot['block_timestamp'] >= start_date) & (df_to_plot['block_timestamp'] <= end_date)]
        df_to_source = df_to_source.groupby(['addr']).agg({'block_number': 'count'}).reset_index()
        df_to_source['percentage'] = 100 * df_to_source['block_number']/df_to_source['block_number'].sum()

        return df_to_source

    def make_dataset(input_data):
        new_src = ColumnDataSource(data=input_data)
        return new_src

    def make_dataset_topN(input_data, topN):
        if not isinstance(topN,int):
            if isinstance(int(topN),int):
                topN = int(topN)
            else:
                topN = 50
        input_data = input_data.nlargest(topN,'block_number',keep='first')
        input_data.sort_values(by='block_number',ascending=True,inplace=True)
        new_src_topN = ColumnDataSource(data=input_data)
        return new_src_topN

    def make_plot(src):
        y_range = list(src.data['addr'].tolist())

        TOOLS = "pan,wheel_zoom,box_zoom,reset,save,box_select,lasso_select"

        p = figure(y_range=y_range, plot_width=500, plot_height=2500, tools=TOOLS,
                   title="Blocks Mined by Address")
        p.hbar(y='addr', right='block_number', height=.25, left=0, source=src, fill_color="#b2de69")

        # Hover tool with vline mode
        hover = HoverTool(tooltips=[
            ('address', '@addr'),
            ('blocks','@block_number'),
            ('%','@percentage')
        ], mode='vline')

        p.add_tools(hover)

        return p

    def make_plot_topN(src_topN):

        y_range = list(src_topN.data['addr'].tolist())

        p1 = figure(y_range=y_range, plot_width=500, plot_height=1000, tools=TOOLS,
                   title="Top N Blocks Mined by Address")
        p1.hbar(y='addr', right='block_number', left=0, height=0.5, source=src_topN, fill_color="#b2de69")

        # Hover tool with vline mode
        hover = HoverTool(tooltips=[
            ('address', '@addr'),
            ('blocks', '@block_number'),
            ('%', '@percentage')
        ], mode='vline')

        p1.add_tools(hover)

        return p1

    def update(attr, old, new):
        #filter the data
        df_to_plot = df[['block_timestamp','addr','block_number']]
        start = date_range_choose.value[0]
        end = date_range_choose.value[1]
        if isinstance(int(text_input.value),int):
            update_val = int(text_input.value)
        else:
            update_val = 50

        #update the source
        df_result= prep_dataset(df_to_plot, start, end)
        src.data.update(make_dataset(df_result).data)
        src_topN.data.update(make_dataset_topN(df_result,update_val).data)



    #create a slider widget
    first_date = "2018-7-5 00:00:00"
    # multiply by 1000 to convert to milliseconds
    first_date = datetime.strptime(first_date, "%Y-%m-%d %H:%M:%S").timestamp()*1000
    last_date = datetime.now().timestamp()*1000
    date_range_choose = DateRangeSlider(title="Select Date Range ", start=first_date, end=last_date,
                                        value=(first_date,last_date), step=1)

    # add a callback to its value
    date_range_choose.on_change('value',update)


    #create a text widget for top N
    text_input = TextInput(value='50', title="Top N Miners (Max 50):")
    text_input.on_change("value", update)

    # INITIAL SETUP
    initial_df_to_plot = df[['block_timestamp','addr', 'block_number']]

    # make the datasource
    df_output = prep_dataset(initial_df_to_plot,
                       date_range_choose.value[0],
                       date_range_choose.value[1])


    src=make_dataset(df_output)
    src_topN=make_dataset_topN(df_output,text_input.value)

    #make the plot
    p = make_plot(src)
    p1 = make_plot_topN(src_topN)

    # put the controls in a single element
    controls = WidgetBox(date_range_choose,text_input)

    # Create the dashboard
    grid = gridplot([[controls, p, p1],[None]])

    # Make a tab with the layout
    tab = Panel(child=grid, title='Poolminer')

    return tab