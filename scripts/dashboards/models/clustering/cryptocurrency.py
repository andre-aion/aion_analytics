from datetime import datetime,timedelta
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Spacer

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.utils.interfaces.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import string_contains_list
from config.dashboard import config as dashboard_config

from tornado.gen import coroutine
from sklearn.preprocessing import StandardScaler  # For scaling dataset
from sklearn.cluster import KMeans  #For clustering

import numpy as np
import pandas as pd
import holoviews as hv
from holoviews import streams

from scripts.utils.myutils import tab_error_flag
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

table = 'crypto_modelling'
groupby_dict = {
    'watch': 'sum',
    'fork': 'sum',
    'release': 'sum',
    'push': 'sum',
    'close': 'mean',
    'high': 'mean',
    'low': 'mean',
    'market_cap': 'mean',
    'volume': 'mean',

}

@coroutine
def cryptocurrency_clustering_tab(panel_title):

    class Thistab(Mytab):
        def __init__(self, table, cols,dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.cols = cols
            self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
            self.df = None
            self.df1 = None
            self.df_predict = None
            self.day_diff = 1  # for normalizing for classification periods of different lengths
            self.df_grouped = ''

            self.cl = PythonClickhouse('aion')
            # add all the coins to the dict
            self.github_cols = ['watch','fork','issue','release','push']
            self.index_cols = ['close','high','low','market_cap','volume']

            self.trigger = 0

            self.groupby_dict = groupby_dict
            self.feature_list = list(self.groupby_dict.keys())
            self.kmean_model = {}

            self.div_style = """ style='width:350px; margin-left:25px;
                            border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                            """

            self.header_style = """ style='color:blue;text-align:center;' """

            self.k = '1'
            self.max_clusters_menu = [str(k) for k in range(1, 12)]

            self.launch_cluster_table = False # launch cluster
            self.cryptos = None
            # ------- DIVS setup begin
            self.page_width = 1200
            txt = """<hr/><div style="text-align:center;width:{}px;height:{}px;
                        position:relative;background:black;margin-bottom:200px">
                        <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                  </div>""".format(self.page_width, 50, 'Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=self.page_width, height=20),
                'bottom': Div(text=txt, width=self.page_width, height=10),
            }

            self.section_divider = '-----------------------------------'
            self.section_headers = {
                'Crypto families': self.section_header_div(
                    text='Crypto families:{}'.format(self.section_divider),
                    width=600, html_header='h2', margin_top=5,margin_bottom=-155),

            }

        # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)


        # //////////////  DIVS   /////////////////////////////////

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        def information_div(self, width=400, height=150):
            div_style = """ 
               style='width:350px;
               border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
           """
            txt = """
            <div {}>
            <h4 {}>How to interpret relationships </h4>
            <ul style='margin-top:-10px;'>
                <li>
                A cluster is statistical grouping of items based on a composite similarity of the variables under review. 
                
                </li>
                <li>
                I have highlighted the peers in our cluster (aion_cluster), and simply labeled the other clusters with numbers.
                </li>
            </ul>
            </div>

            """.format(div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div
        # ////////////////// HELPER FUNCTIONS ////////////////////
        def set_groupby_dict(self):
            try:
                lst = ['mention','hashtags','tweets','replies','favorites']
                for col in self.cols:
                    if col not in self.groupby_dict.keys():
                        if not string_contains_list(lst,col):
                            self.groupby_dict[col] = 'mean'
                        else:
                            self.groupby_dict[col] = 'sum'

            except Exception:
                logger.error('set groupby dict',exc_info=True)


        # /////////////////////////////////////////////////////////////
        def optimalK(self, data, nrefs=3, maxClusters=10):
            try:
                """
                Calculates KMeans optimal K using Gap Statistic from Tibshirani, Walther, Hastie
                Params:
                    data: ndarry of shape (n_samples, n_features)
                    nrefs: number of sample reference datasets to create
                    maxClusters: Maximum number of clusters to test for
                Returns: (gaps, optimalK)
                """
                gaps = np.zeros((len(range(1, maxClusters)),))
                resultsdf = pd.DataFrame({'clusterCount': [], 'gap': []})
                for gap_index, k in enumerate(range(1, len(self.max_clusters_menu))):
                    logger.warning('starting for k=%s',k)
                    # Holder for reference dispersion results
                    refDisps = np.zeros(nrefs)

                    # For n references, generate random sa,kmple and perform kmeans getting resulting dispersion of each loop
                    for i in range(nrefs):
                        logger.warning('nref=%s',i)

                        # Create new random reference set
                        randomReference = np.random.random_sample(size=data.shape)

                        # Fit to it
                        km = KMeans(k)
                        km.fit(randomReference)

                        refDisp = km.inertia_
                        refDisps[i] = refDisp

                    # Fit cluster to original data and create dispersion

                    self.kmean_model[k] = KMeans(k, random_state=42)
                    self.kmean_model[k].fit(data)

                    origDisp = km.inertia_

                    # Calculate gap statistic
                    gap = np.log(np.mean(refDisps)) - np.log(origDisp)

                    # Assign this loop's gap statistic to gaps
                    gaps[gap_index] = gap

                    resultsdf = resultsdf.append({'clusterCount': k, 'gap': gap}, ignore_index=True)

                return (gaps.argmax() + 1,
                        resultsdf)  # Plus 1 because index of 0 means 1 cluster is optimal, index 2 = 3 clusters are optimal

            except Exception:
                logger.error('optimal', exc_info=True)

        def cluster_table(self, launch):
            try:
                # prep
                df = self.df.groupby(['crypto']).agg(groupby_dict)
                df = df.compute()
                logger.warning('df after groupby:%s',df)

                self.cryptos = df.index.tolist()
                logger.warning('self.cryptos:%s',self.cryptos)
                print(self.cryptos)

                X = df[self.feature_list]
                scaler = StandardScaler()
                X = scaler.fit_transform(X)
                self.k, gapdf = self.optimalK(X, nrefs=3, maxClusters=len(self.max_clusters_menu))
                logger.warning('Optimal k is:%s ', self.k)
                # Labels of each point
                labels = self.kmean_model[self.k].labels_

                # Nice Pythonic way to get the indices of the points for each corresponding cluster
                mydict = {'cluster_' + str(i): np.where(labels == i)[0].tolist() for i in range(self.kmean_model[self.k].n_clusters)}
                mydict_verbose = mydict.copy() # make a dictionary with the clusters and name of the cryptos

                # Transform this dictionary into dct with matching crypto labels
                dct = {
                    'crypto': self.cryptos,
                    'cluster': [''] * len(self.cryptos)
                }
                # get index aion to identify the aion cluster
                aion_idx =  self.cryptos.index('aion')

                for key, values in mydict.items():
                    if aion_idx in values:
                        key = 'aion_cluster'
                    mydict_verbose[key] = []
                    for crypto_index in values:
                        try:
                            dct['cluster'][int(crypto_index)] = key
                            mydict_verbose[key].append(self.cryptos[int(crypto_index)])

                        except:
                            logger.warning('cannot change to int:%s',crypto_index)# save to redis
                self.write_clusters(mydict_verbose)
                logger.warning('line 229: cluster labels:%s', mydict_verbose)

                df = pd.DataFrame.from_dict(dct)
                self.launch_cluster_table = False
                cols = ['crypto', 'cluster']
                return df.hvplot.table(columns=cols, width=500, height=1200, title='Cluster table')
            except Exception:
                logger.error('cluster table', exc_info=True)


        def write_clusters(self,my_dict):
            try:
                # write to redis
                cluster_dct = my_dict.copy()
                cluster_dct['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
                cluster_dct['features'] = self.feature_list
                save_params = 'clusters:cryptocurrencies'
                self.redis.save(cluster_dct,
                                save_params,
                                "", "", type='checkpoint')
                logger.warning('%s saved to redis',save_params)
            except:
                logger.error('',exc_info=True)



    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.df_load(datepicker_start.value, datepicker_end.value,timestamp_col='timestamp')
        thistab.trigger += 1
        stream_launch_elbow_plot.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    try:
        # SETUP
        table = 'external_daily'
        #cols = list(groupby_dict.keys()) + ['crypto']
        thistab = Thistab(table,[],[])

        # setup dates
        first_date_range = datetime.strptime("2018-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date'] - timedelta(days=2)
        first_date = last_date - timedelta(days=340)
        # initial function call
        thistab.df_load(first_date, last_date,timestamp_col='timestamp')
        thistab.cols = sorted(list(thistab.df.columns))


        # MANAGE STREAMS
        stream_launch_elbow_plot = streams.Stream.define('Launch_elbow_plot', launch=-1)()
        stream_launch_cluster_table = streams.Stream.define('Launch_cluster_table', launch=-1)()


        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                  max_date=last_date_range, value=first_date)

        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                max_date=last_date_range, value=last_date)

        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)

        # PLOTS
        hv_cluster_table = hv.DynamicMap(thistab.cluster_table,
                                      streams=[stream_launch_cluster_table])
        cluster_table = renderer.get_plot(hv_cluster_table)

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls= WidgetBox(
            datepicker_start,
            datepicker_end
        )

        # create the dashboards

        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.information_div(),controls],
            [thistab.section_headers['Crypto families']],
            [Spacer(width=20, height=30)],
            [cluster_table.state],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('crypto:', exc_info=True)
        return tab_error_flag(panel_title)