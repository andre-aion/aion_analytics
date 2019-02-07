from concurrent.futures import ThreadPoolExecutor

from bokeh.models import WidgetBox
from bokeh.plotting import figure
from tornado import gen
from bokeh.document import without_document_lock

# Bokeh basics
from bokeh.models.widgets import Tabs, CheckboxGroup, Button, Panel, Div
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.layouts import row, gridplot

# GET THE DASHBOARDS
from tornado.ioloop import IOLoop

from scripts.dashboards.account_activity import account_activity_tab
from scripts.dashboards.blockminer import blockminer_tab
from scripts.dashboards.poolminer import poolminer_tab
from scripts.dashboards.churn.churn import churn_tab
from scripts.dashboards.churn.tier1_miner_predictive import tier1_miner_churn_predictive_tab
from scripts.dashboards.churn.tier2_miner_predictive import tier2_miner_churn_predictive_tab
from scripts.dashboards.churn.account_activity_predictive import account_activity_predictive_tab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)


labels = [
    'blockminer_tab',
    'poolminer_tab',
    'account_activity_tab',
    'account_activity_predictive_tab',
    'churn_tab',
    'tier1_miner_churn_predictive_tab',
    'tier2_miner_churn_predictive_tab',
    ]


class SelectionTab:
    def __init__(self):

        self.selected_tabs = []
        self.tablist = []
        self.selected_tracker = [] # used to monitor if a tab has already been launched
        self.div_style = """ style='width:300px; margin-left:25px;
                   border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                   """
        self.header_div = Div(text="""<div style="text-align:center;background:black;width:100%;">
                    <h1 style="color:#fff;">
                    {}</h1></div>""", width=1200, height=20)
        txt = """<div style="text-align:center;background:black;width:100%;">
                                    <h1 style="color:#fff;">
                                    {}</h1></div>""".format('Welcome to Aion Data Science Portal')
        self.notification_div = Div(text=txt, width=1200, height=20)

    def notification_updater(self, text):
        txt = """<div style="text-align:center;background:black;width:100%;">
                                                                             <h4 style="color:#fff;">
                                                                             {}</h4></div>""".format(text)
        self.notification_div.text = txt

    def get_selections(self,checkboxes):
        self.selected_tabs= [checkboxes.labels[i] for i in checkboxes.active]
        return self.selected_tabs



selection_tab = SelectionTab()

@gen.coroutine
def aion_analytics(doc):
    # SETUP BOKEH OBJECTS
    try:
        selection_tab.tablist = []
        TABS = Tabs(tabs=selection_tab.tablist)
        @gen.coroutine
        def load_callstack():
            lst = selection_tab.get_selections(selection_checkboxes)
            logger.warning('selections:%s',lst)

            if 'blockminer_tab' in lst:
                if 'blockiminer_tab' not in selection_tab.selected_tracker:
                    bm = yield blockminer_tab()
                    selection_tab.selected_tracker.append('blockminer_tab')
                    if bm not in selection_tab.tablist:
                        selection_tab.tablist.append(bm)


            if 'poolminer_tab' in lst:
                if 'poolminer_tab' not in selection_tab.selected_tracker:
                    pm = yield poolminer_tab()
                    selection_tab.selected_tracker.append('poolminer_tab')
                    if pm not in selection_tab.tablist:
                        selection_tab.tablist.append(pm)

            if 'churn_tab' in lst:
                if 'churn_tab' not in selection_tab.selected_tracker:
                    ch = yield churn_tab()
                    selection_tab.selected_tracker.append('churn_tab')
                    if ch not in selection_tab.tablist:
                        selection_tab.tablist.append(ch)

            if 'tier1_miner_churn_predictive_tab' in lst:
                if 'tier1_miner_churn_predictive_tab' not in selection_tab.selected_tracker:
                    mch_1 = yield tier1_miner_churn_predictive_tab()
                    selection_tab.selected_tracker.append('tier1_miner_churn_predictive_tab')
                    if mch_1 not in selection_tab.tablist:
                        selection_tab.tablist.append(mch_1)

            if 'tier2_miner_churn_predictive_tab' in lst:
                if 'tier2_miner_churn_predictive_tab' not in selection_tab.selected_tracker:
                    mch_2 = yield tier2_miner_churn_predictive_tab()
                    selection_tab.selected_tracker.append('tier2_miner_churn_predictive_tab')
                    if mch_2 not in selection_tab.tablist:
                        selection_tab.tablist.append(mch_2)

            if 'account_activity_predictive_tab' in lst:
                if 'account_activity_predictive_tab' not in selection_tab.selected_tracker:
                    aap = yield account_activity_predictive_tab()
                    selection_tab.selected_tracker.append('account_activity_predictive_tab')
                    if aap not in selection_tab.tablist:
                        selection_tab.tablist.append(aap)

            if 'account_activity_tab' in lst:
                if 'account_activity_tab' not in selection_tab.selected_tracker:
                    aa = yield account_activity_tab()
                    selection_tab.selected_tracker.append('account_activity_tab')
                    if aa not in selection_tab.tablist:
                        selection_tab.tablist.append(aa)

            # make list unique
            selection_tab.tablist = list(set(selection_tab.tablist))
            TABS.update(tabs=selection_tab.tablist)

        @gen.coroutine
        def select_tabs():
            notification_div.text = """<div style="text-align:center;background:black;width:100%;">
                                    <h1 style="color:#fff;">
                                    {}</h1></div>""".format('Please be patient. Tabs are loading..')
            yield load_callstack()
            notification_div.text = """<div style="text-align:center;background:black;width:100%;">
                                                <h1 style="color:#fff;">
                                                {}</h1></div>""".format('Welcome to Aion Data Science Portal')
        txt = """
                <div {}>
                    <h3 style='color:blue;text-align:center'>Info:</h3>
                    <ul style='margin-top:-10px;height:200px'>
                    <li>
                    Select the tab(s) you want activated
                    </li>
                    <li>
                    The click the 'launch activity' button.
                    </li>
                    </ul>
                </div>
                """.format(selection_tab.div_style)

        information_div = Div(text=txt, width=400, height=400)
        buffer_div = Div(text='', width=300, height=20)
        footer_div = Div(text='<hr/><div style="width:100%;height:100px;position:relative;background:black;"></div>',
                         width=1200, height=100)
        txt = """<div style="text-align:center;background:black;width:100%;">
                                                <h1 style="color:#fff;">
                                                {}</h1></div>""".format('Welcome to Aion Data Science Portal')
        notification_div = Div(text=txt,width=1200,height=20)
        selection_checkboxes = CheckboxGroup(labels=labels, active=[2])
        run_button = Button(label='Launch tabs', button_type="success")
        run_button.on_click(select_tabs)

        controls = WidgetBox(selection_checkboxes, run_button)

        # create the dashboards
        grid = gridplot([
            [notification_div],
            [buffer_div,controls, information_div],
            [footer_div]
        ])
        mgmt = Panel(child=grid, title='Tab Selection')

        selection_tab.tablist.append(mgmt)
        TABS.update(tabs=selection_tab.tablist)
        #tabs.on_change()
        doc.add_root(TABS)
    except Exception:
        logger.error("TABS:", exc_info=True)

# configure and run bokeh server
@gen.coroutine
@without_document_lock
def launch_server():
    try:
        apps = {"/aion-analytics": Application(FunctionHandler(aion_analytics))}
        io_loop = IOLoop.current()
        server = Server(apps,port=5006, allow_websocket_origin=["*"],io_loop=io_loop,
                        session_ids='signed',relative_urls=False)
        server.start()
        server.io_loop.add_callback(server.show, '/aion-analytics')
        server.io_loop.start()
    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)

if __name__ == '__main__':
    launch_server()