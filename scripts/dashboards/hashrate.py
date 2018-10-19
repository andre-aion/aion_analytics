from scripts.utils.hashrate import calc_hashrate
from bokeh.plotting import figure, output_file, show
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d, \
    PanTool, SaveTool, ResetTool, DataRange1d, Plot, LinearAxis
from bokeh.models.glyphs import Line
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
import gc
import pandas as pd

from pdb import set_trace

def hashrate_tab(df):
    def make_dataset(blockcount):
        input_df = calc_hashrate(df, blockcount)
        src = ColumnDataSource(data=dict(x=input_df['block_number'].compute(),
                                         y=input_df['hashrate'].compute()))
        # clean up operations
        del input_df
        gc.collect()
        return src


    def make_plot(src):
        title='Hashrate with adjustable number of blocks for mean of blocktime'
        TOOLS=[PanTool(),SaveTool(),ResetTool()]
        p = figure(title=title, plot_width=1200, plot_height=500, tools=TOOLS)

        p.line(x='x', y='y', line_color="#f46d43", line_width=1,
               line_alpha=0.6, source=src)

        p.xaxis.axis_label = 'blocknumber'
        p.yaxis.axis_label = 'hashrate(sols)'

        # Hover tool with vline mode
        hover = HoverTool(tooltips=[
            ('blocks', '@x'),
            ('hashrate', '@y')
        ], mode='vline')

        p.add_tools(hover)

        return p

    def update(attr, old, new):
        blockcount = blockcount_slider.value
        #update the source
        src.data.update(make_dataset(blockcount).data)



    # create a slider widget
    initial_blockcount = 500
    blockcount_slider = Slider(start=100, end=15000, value=initial_blockcount, step=100)

    # add callback
    blockcount_slider.on_change("value", update)

    # make the plot
    src = make_dataset(blockcount_slider.value)
    p = make_plot(src)

    # put the controls in a single element
    controls = WidgetBox(blockcount_slider)


    #create the dashboard
    grid = gridplot([[controls, p],[None]])

    # Make a tab with the layout
    tab = Panel(child=grid, title='Hashrate')

    return tab
