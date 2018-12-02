import config

#import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d
import gc
from bokeh.io import curdoc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider, Div
from holoviews import streams
from holoviews.streams import Stream,RangeXY,RangeX,RangeY, Pipe
from pdb import set_trace
import hvplot.dask
import hvplot.pandas

import holoviews as hv, param, dask.dataframe as dd
from holoviews.operation.datashader import rasterize, shade, datashade
from datetime import datetime
import numpy as np
import pandas as pd
import dask as dd

from dask.distributed import Client
from dask import visualize, delayed

import holoviews as hv
hv.extension('bokeh',logo=False)

def poolminer_tab():
    print('IN POOLMINER')

    # Make a tab with the layout
    div = Div(text="""Your <a href="https://en.wikipedia.org/wiki/HTML">HTML</a>-supported text is initialized with the <b>text</b> argument.  The
    remaining div arguments are <b>width</b> and <b>height</b>. For this example, those values
    are <i>200</i> and <i>100</i> respectively.""",
    width=200, height=100)

    tab = Panel(child=div, title='Poolminer')
    
    
    
    
    return tab

'''
def poolminer_tab():
    df=config.block_df
    class poolminer():
        df1 = ''
        n = 20
        def prep_dataset(self, start_date, end_date):
            self.df1 = ''
            # change from milliseconds to seconds
            if start_date > 1630763200:
                start_date = (start_date // 1000)
            if end_date > 1630763200:
                end_date = (end_date // 1000)

            start_date = datetime.fromtimestamp(start_date)
            end_date = datetime.fromtimestamp(end_date)
            #set df1 while outputting bar graph
            self.df1 = df.loc[start_date:end_date, :]

            self.df1 = self.df1.groupby('addr').count()
            self.df1['percentage'] = 100*self.df1['block_number']/self.df1['block_number'].sum()
            self.df1 = self.df1.reset_index()
            return hv.Bars(self.df1,hv.Dimension('addr'),'block_number')


        def view_topN(self,n):
            # change n from string to int
            if isinstance(n, int):
                pass
            else:
                try:
                    n = int(n)
                except:
                    print("n cannot be changed into an int")
                    n = 30
            self.set_n(n)
            df2 = self.df1.nlargest(n,'percentage').reset_index().compute()
            bars_n = hv.Bars(df2,kdims=['addr'],vdims=['percentage'])

            del df2
            gc.collect()
            return bars_n

        def set_n(self,n):
            self.n = n

    def update_dates(attrname, old, new):
        # notify the holoviews stream of the slider update
        stream_dates.event(start_date=new[0],end_date=new[1])

    def update_topN(attrname, old,new):
        stream_topN.event(n=new)


    # STREAMS
    #format dates
    first_date = "2018-7-5 00:00:00"
    # multiply by 1000 to convert to milliseconds
    first_date = datetime.strptime(first_date, "%Y-%m-%d %H:%M:%S").timestamp()*1000
    last_date = datetime.now().timestamp()*1000

    stream_dates = streams.Stream.define('Dates',start_date=first_date, end_date=last_date)()
    stream_topN = streams.Stream.define('TopN',n=40)()

    # MANAGE
    date_range_choose = DateRangeSlider(title="Select Date Range ", start=first_date, end=last_date,
                                        value=(first_date, last_date), step=1)
    # create a text widget for top N
    text_input = TextInput(value='50', title="Top N Miners (Max 50):")

    # add callbacks
    date_range_choose.on_change('value',update_dates)
    text_input.on_change("value", update_topN)

    # create plot and render to bokeh
    pm = poolminer()
    dmap_all = hv.DynamicMap(pm.prep_dataset, streams=[stream_dates])
    #dmap_topN = hv.DynamicMap(pm.view_topN, streams=[stream_topN])
    p_all = datashade(dmap_all).opts(plot=dict(height=500, width=1600))
    #p_topN = datashade(dmap_topN).opts(plot=dict(height=500, width=500))

    renderer = hv.renderer('bokeh')
    hvmap_all=renderer.get_plot(p_all)
    #hvmap_topN=renderer.get_plot(p_topN)


    # put the controls in a single element
    controls = WidgetBox(date_range_choose, text_input)

    # create the dashboard
    grid = gridplot([[controls], [hvmap_all.state]])

    # Make a tab with the layout
    tab = Panel(child=grid, title='Poolminer')
    return tab
'''