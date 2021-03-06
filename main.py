from concurrent.futures import ThreadPoolExecutor

from bokeh.models import WidgetBox, Spacer
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

from scripts.dashboards.EDA.BCC.rentals import eda_bcc_rentals_visitor_tab
from scripts.dashboards.EDA.account_activity import account_activity_tab
from scripts.dashboards.EDA.blockminer import blockminer_tab
from scripts.dashboards.EDA.cryptocurrency_clusters import crypto_clusters_eda_tab
from scripts.dashboards.EDA.economic_indicators import eda_country_indexes_tab
from scripts.dashboards.EDA.projects import eda_projects_tab
from scripts.dashboards.KPI.BCC.rentals import kpi_bcc_rentals_visitor_tab
from scripts.dashboards.KPI.business_events import EDA_business_events_tab
from scripts.dashboards.PROJECT_MGMT.risk_assessment import pm_risk_assessment_tab
from scripts.dashboards.KPI.projects import KPI_projects_tab
from scripts.dashboards.KPI.social_media import KPI_social_media_tab
from scripts.dashboards.TSA.BCC.rentals import forecasting_bcc_rentals_visitor_tab
from scripts.dashboards.TSA.accounts import accounts_tsa_tab
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

'''
labels = [
    'PROJECT MGMT: Risk assessment',
    'KPI: user adoption',
    'KPI: developer adoption',
    #'KPI: social media',
    'KPI: projects',
    'EDA: business events',
    'EDA: miners',
    'EDA: account activity',
    'EDA: cryptocurrencies',
    'EDA: crypto clusters',
    'EDA: projects',
    #'EDA: country indexes',
    'clustering: cryptocurrencies',
    'predictions: accounts by value',
    'forecasting: accounts',
    'EDA: Bar',

    ]
'''
labels = [
    'KPI: Rentals',
    'EDA: Rentals',
    'Predictive: Rentals',
]
DEFAULT_CHECKBOX_SELECTION = 0

@gen.coroutine
def aion_analytics(doc):
    class SelectionTab:
        def __init__(self):
            self.selected_tabs = []
            self.tablist = []
            self.selected_tracker = []  # used to monitor if a tab has already been launched
            self.div_style = """ style='width:300px; margin-left:-200%;
                       border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                       """
            self.page_width = 1200

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

            panel_title = 'EDA: crypto clusters'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    eda_cc = yield crypto_clusters_eda_tab(cryptocurrencies,panel_title=panel_title)
                    selection_tab.selected_tracker.append(panel_title)
                    if eda_cc not in tablist:
                        tablist.append(eda_cc)

            panel_title = 'clustering: cryptocurrencies'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    cct = yield cryptocurrency_clustering_tab(panel_title)
                    selection_tab.selected_tracker.append(panel_title)
                    if cct not in tablist:
                        tablist.append(cct)


            if 'predictions: accounts by value' in lst:
                if 'predictions: accounts by value' not in selection_tab.selected_tracker:
                    ap = yield account_predictive_tab(page_width=selection_tab.page_width)
                    selection_tab.selected_tracker.append('predictions: accounts by value')
                    if ap not in tablist:
                        tablist.append(ap)

            panel_title = 'Forecasting: Rentals'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    tsa = yield forecasting_bcc_rentals_visitor_tab(panel_title)
                    selection_tab.selected_tracker.append(panel_title=panel_title)
                    if tsa not in tablist:
                        tablist.append(tsa)

            panel_title = 'KPI: Rentals'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    rentals = yield kpi_bcc_rentals_visitor_tab(panel_title=panel_title)
                    selection_tab.selected_tracker.append(panel_title)
                    if rentals not in tablist:
                        tablist.append(rentals)

            panel_title = 'EDA: Rentals'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    rentals = yield eda_bcc_rentals_visitor_tab(panel_title=panel_title)
                    selection_tab.selected_tracker.append(panel_title)
                    if rentals not in tablist:
                        tablist.append(rentals)

            # make list unique
            tablist = list(set(tablist))
            TABS.update(tabs=tablist)

        @gen.coroutine
        def select_tabs():
            notification_div.text = """
                <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                        <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                </div>""".format(selection_tab.page_width,'Tabs are loading')
            yield load_callstack(tablist)
            notification_div.text = """
                <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                        <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                </div>""".format(selection_tab.page_width,'Welcome to BCC Data Science Portal')
        @gen.coroutine
        def update_selected_tabs():
            notification_div.text = """
                <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                        <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                </div>""".format(selection_tab.page_width,'Refresh underway')

            doc.clear()
            tablist = []
            selection_checkboxes.active=[]

            mgmt = Panel(child=grid, title='Tab Selection')
            tablist.append(mgmt)
            TABS.update(tabs=tablist)
            doc.add_root(TABS)
            yield load_callstack(tablist)
            notification_div.text = """
                <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                        <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                </div>""".format(selection_tab.page_width,'Welcome to Aion Data Science Portal')


        # -----------------------
        txt = """
                <div {}>
                <h3 style='color:blue;text-align:center'>Info:</h3>
                <ul style='margin-top:-10px;height:200px;'>
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
        footer_div = Div(text="""<hr/><div style="width:{}px;height:{}px;
                              position:relative;background:black;"></div>"""
                         .format(selection_tab.page_width,50),
                         width=selection_tab.page_width, height=100)
        txt = """
            <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                    <h1 style="color:#fff;margin-bottom:300px">{}</h1>
            </div>""".format(selection_tab.page_width,'Welcome to Aion Data Science Portal')
        notification_div = Div(text=txt,width=selection_tab.page_width,height=40)

        # choose startup tabs
        selection_checkboxes = CheckboxGroup(labels=labels, active=[DEFAULT_CHECKBOX_SELECTION])
        run_button = Button(label='Launch tabs', button_type="success")
        run_button.on_click(select_tabs)

        # setup layout
        controls = WidgetBox(selection_checkboxes, run_button)

        # create the dashboards
        grid = gridplot([
            [notification_div],
            [Spacer(width=50, height=2, sizing_mode='scale_width')],
            [controls,information_div],
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
        apps = {"/analytics": Application(FunctionHandler(aion_analytics))}
        io_loop = IOLoop.current()
        server = Server(apps,port=5006, allow_websocket_origin=["*"],io_loop=io_loop,
                        session_ids='signed',relative_urls=False)
        server.start()
        server.io_loop.add_callback(server.show, '/analytics')
        server.io_loop.start()
    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)

if __name__ == '__main__':
    launch_server()