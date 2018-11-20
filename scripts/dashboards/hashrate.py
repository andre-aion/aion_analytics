import holoviews as hv, param, dask.dataframe as dd
import panel as pp
from holoviews.operation.datashader import rasterize, shade, datashade
from bokeh.document import Document
hv.extension('bokeh', logo=False)
from scripts.utils.hashrate import calc_hashrate
import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d
import gc
from bokeh.io import curdoc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider
from holoviews import streams
from holoviews.streams import Stream,RangeXY,RangeX,RangeY, Pipe
from pdb import set_trace

def hashrate_tab(df):

    def view(bcount):
        input_df = calc_hashrate(df, bcount).compute()
        opts = dict(sublabel_format="", yaxis='bare',
                    xaxis='bare', aspect=1, axiswise=True)
        curve = hv.Curve(input_df, kdims=['block_number'], vdims=['hashrate'])\
            .options(width=1000,height=600)
        del input_df
        gc.collect()
        return curve

    def update(attrname, old, new):
        # notify the holoviews stream of the slider update
        stream.event(bcount=new)

    # MANAGE STREAM
    stream = streams.Stream.define('Blockcount', bcount=500)()

    # create a slider widget
    initial_blockcount = 500
    blockcount_slider = Slider(start=100, end=10000, value=initial_blockcount, step=100, title='Blockcount')

    # create plot and render to bokeh
    dmap = hv.DynamicMap(view, streams=[stream])
    p = datashade(dmap).opts(plot=dict(height=500,width=800))

    blockcount_slider.on_change("value", update)
    renderer = hv.renderer('bokeh')
    hvmap = renderer.get_plot(p)

    # put the controls in a single element
    controls = WidgetBox(blockcount_slider)

    # create the dashboard
    grid = gridplot([[controls,hvmap.state], [None]])

    # Make a tab with the layout
    tab = Panel(child=grid, title='Hashrate')

    return tab

