from concurrent.futures import ThreadPoolExecutor

from bokeh.models import WidgetBox
from tornado import gen
from bokeh.document import without_document_lock

# Bokeh basics
from bokeh.models.widgets import Tabs, CheckboxGroup, Button, Panel, Div
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.layouts import gridplot

# GET THE DASHBOARDS
from tornado.ioloop import IOLoop

from scripts.dashboards.EDA.account_activity import account_activity_tab
from scripts.dashboards.EDA.blockminer import blockminer_tab
from scripts.dashboards.EDA.cryptocurrency_clusters import crypto_clusters_eda_tab
from scripts.dashboards.KPI.social_media import KPI_social_media_tab
from scripts.dashboards.models.predictive.account_activity_predictive import account_activity_predictive_tab
from scripts.dashboards.models.predictive.account_predictive import account_predictive_tab
from scripts.dashboards.KPI.developer_adoption import KPI_developer_adoption_tab
from scripts.dashboards.EDA.cryptocurrency import cryptocurrency_eda_tab
from scripts.dashboards.models.clustering.cryptocurrency import cryptocurrency_clustering_tab
from scripts.dashboards.KPI.user_adoption import KPI_user_adoption_tab

from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import load_cryptos

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)
cryptocurrencies = load_cryptos()


labels = [
    'KPI: user adoption',
    'KPI: developer adoption',
    'KPI: social media',
    'miners: blocks',
    'EDA: account activity',
    'EDA: cryptocurrency',
    'EDA: crypto clusters',
    'clustering: cryptocurrency',
    'predictions: accounts by value',
    ]
DEFAULT_CHECKBOX_SELECTION = 2

@gen.coroutine
def aion_analytics(doc):
    class SelectionTab:
        def __init__(self):
            self.selected_tabs = []
            self.tablist = []
            self.selected_tracker = []  # used to monitor if a tab has already been launched
            self.div_style = """ style='width:300px; margin-left:25px;
                       border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                       """

            self.txt = """<div style="text-align:center;background:black;width:100%;">
                                        <h1 style="color:#fff;">
                                        {}</h1></div>""".format('Welcome to Aion Data Science Portal')

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                                 <h4 style="color:#fff;">
                                                                                 {}</h4></div>""".format(text)
            return txt

        def get_selections(self, checkboxes):
            self.selected_tabs = [checkboxes.labels[i] for i in checkboxes.active]
            return self.selected_tabs

    selection_tab = SelectionTab()
    # SETUP BOKEH OBJECTS
    try:
        tablist = []
        TABS = Tabs(tabs=tablist)
        @gen.coroutine
        def load_callstack(tablist):
            lst = selection_tab.get_selections(selection_checkboxes)
            #logger.warning('selections:%s',lst)

            if 'KPI: user adoption' in lst:
                if 'KPI: user adoption' not in selection_tab.selected_tracker:
                    kpi_user_adoption = yield KPI_user_adoption_tab()
                    selection_tab.selected_tracker.append('KPI: user adoption')
                    if kpi_user_adoption not in tablist:
                        tablist.append(kpi_user_adoption)

            if 'KPI: developer adoption' in lst:
                if 'KPI: developer adoption' not in selection_tab.selected_tracker:
                    developer_user_adoption = yield KPI_developer_adoption_tab()
                    selection_tab.selected_tracker.append('KPI: developer adoption')
                    if developer_user_adoption not in tablist:
                        tablist.append(developer_user_adoption)

            panel_title = 'KPI: social media'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    sm = yield KPI_social_media_tab(panel_title=panel_title)
                    selection_tab.selected_tracker.append(panel_title)
                    if sm not in tablist:
                        tablist.append(sm)

            if 'miners: blocks' in lst:
                if 'miners: blocks' not in selection_tab.selected_tracker:
                    bm = yield blockminer_tab()
                    selection_tab.selected_tracker.append('miners: blocks')
                    if bm not in tablist:
                        tablist.append(bm)


            panel_title = 'EDA: account activity'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    aa = yield account_activity_tab(panel_title=panel_title)
                    selection_tab.selected_tracker.append(panel_title)
                    if aa not in tablist:
                        tablist.append(aa)

            if 'EDA: cryptocurrency' in lst:
                if 'EDA: cryptocurrency' not in selection_tab.selected_tracker:
                    eda_c = yield cryptocurrency_eda_tab(cryptocurrencies)
                    selection_tab.selected_tracker.append('EDA: cryptocurrency')
                    if eda_c not in tablist:
                        tablist.append(eda_c)

            panel_title = 'EDA: crypto clusters'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    eda_cc = yield crypto_clusters_eda_tab(cryptocurrencies,panel_title=panel_title)
                    selection_tab.selected_tracker.append(panel_title)
                    if eda_cc not in tablist:
                        tablist.append(eda_cc)


            if 'clustering: cryptocurrency' in lst:
                if 'clustering: cryptocurrency' not in selection_tab.selected_tracker:
                    cct = yield cryptocurrency_clustering_tab()
                    selection_tab.selected_tracker.append('clustering: cryptocurrency')
                    if cct not in tablist:
                        tablist.append(cct)

            panel_title = 'EDA: account activity'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    aap = yield account_activity_predictive_tab(panel_title)
                    selection_tab.selected_tracker.append(panel_title)
                    if aap not in tablist:
                        tablist.append(aap)



            if 'predictions: accounts by value' in lst:
                if 'predictions: accounts by value' not in selection_tab.selected_tracker:
                    ap = yield account_predictive_tab()
                    selection_tab.selected_tracker.append('predictions: accounts by value')
                    if ap not in tablist:
                        tablist.append(ap)

            # make list unique
            tablist = list(set(tablist))
            TABS.update(tabs=tablist)

        @gen.coroutine
        def select_tabs():
            notification_div.text = """<div style="text-align:center;background:black;width:100%;">
                                    <h1 style="color:#fff;">
                                    {}</h1></div>""".format('Please be patient. Tabs are loading..')
            yield load_callstack(tablist)
            notification_div.text = """<div style="text-align:center;background:black;width:100%;">
                                                <h1 style="color:#fff;">
                                             {}</h1></div>""".format('Welcome to Aion Data Science Portal')
        @gen.coroutine
        def update_selected_tabs():
            notification_div.text = """<div style="text-align:center;background:black;width:100%;">
                                                <h1 style="color:#fff;">
                                                {}</h1></div>""".format('Refresh underway')

            doc.clear()
            tablist = []
            selection_checkboxes.active=[]

            mgmt = Panel(child=grid, title='Tab Selection')
            tablist.append(mgmt)
            TABS.update(tabs=tablist)
            doc.add_root(TABS)
            yield load_callstack(tablist)
            notification_div.text = """<div style="text-align:center;background:black;width:100%;">
                                                            <h1 style="color:#fff;">
                                                            {}</h1></div>""".format(
                'Welcome to Aion Data Science Portal')


        # -----------------------
        txt = """
                <div {}>
                    <h3 style='color:blue;text-align:center'>Info:</h3>
                    <ul style='margin-top:-10px;height:200px'>
                    <li>
                    Select the tab(s) you want activated
                    </li>
                    <li>
                    Then click the 'launch activity' button.
                    </li>
                    </ul>
                </div>
                """.format(selection_tab.div_style)

        information_div = Div(text=txt, width=400, height=250)
        buffer_div = Div(text='', width=300, height=20)
        footer_div = Div(text='<hr/><div style="width:100%;height:100px;position:relative;background:black;"></div>',
                         width=1200, height=100)
        txt = """<div style="text-align:center;background:black;width:100%;">
                                                <h1 style="color:#fff;">
                                                {}</h1></div>""".format('Welcome to Aion Data Science Portal')
        notification_div = Div(text=txt,width=1200,height=20)
        header_div = Div(text="""<div style="text-align:center;background:black;width:100%;">
                           <h1 style="color:#fff;">
                           {}</h1></div>""", width=1200, height=20)

        # choose startup tabs
        selection_checkboxes = CheckboxGroup(labels=labels, active=[DEFAULT_CHECKBOX_SELECTION])
        run_button = Button(label='Launch tabs', button_type="success")
        run_button.on_click(select_tabs)

        # setup layout
        controls = WidgetBox(selection_checkboxes, run_button)

        # create the dashboards
        grid = gridplot([
            [notification_div],
            [buffer_div,controls, information_div],
            [footer_div]
        ])

        # setup launch tabs
        mgmt = Panel(child=grid, title='Tab Selection')

        tablist.append(mgmt)
        TABS.update(tabs=tablist)
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