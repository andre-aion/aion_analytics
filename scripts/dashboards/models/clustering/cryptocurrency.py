from datetime import datetime,timedelta
import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, Select, TableColumn, ColumnDataSource, DataTable
from bokeh.plotting import figure
from scipy.spatial.distance import cdist

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.utils.dashboards.EDA.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import datetime_to_date
from config.dashboard import config as dashboard_config
from bokeh.models.widgets import CheckboxGroup, TextInput

from tornado.gen import coroutine
from sklearn.preprocessing import StandardScaler  # For scaling dataset
from sklearn.cluster import KMeans, AgglomerativeClustering, AffinityPropagation #For clustering
from sklearn.mixture import GaussianMixture #For GMM clustering linregress
from sklearn.pipeline import Pipeline
from sklearn.metrics import silhouette_samples, silhouette_score

import numpy as np
from operator import itemgetter
import pandas as pd
import dask as dd
import holoviews as hv
import hvplot.pandas
import hvplot.dask
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
def cryptocurrency_clustering_tab():

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
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                           <h1 style="color:#fff;">
                                                                           {}</h1></div>""".format('Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=1400, height=20),
                'bottom':  Div(text=txt, width=1400, height=10),
            }

            self.groupby_dict = groupby_dict
            self.feature_list = list(self.groupby_dict.keys())
            self.kmean_model = {}

            self.div_style = """ style='width:350px; margin-left:25px;
                                    border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                                    """

            self.header_style = """ style='color:blue;text-align:center;' """
            self.section_header_div = {

            }
            self.k = '1'
            self.max_clusters_menu = [str(k) for k in range(1, 12)]

            self.launch_cluster_table = False # launch cluster
            self.cryptos = None


        # ////////////////////////// UPDATERS ///////////////////////
        def section_head_updater(self,section, txt):
            try:
                self.section_header_div[section].text = txt
            except Exception:
                logger.error('',exc_info=True)

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                    <h4 style="color:#fff;">
                    {}</h4></div>""".format(text)
            for key in self.notification_div.keys():
                self.notification_div[key].text = txt


        # //////////////  DIVS   /////////////////////////////////

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        def corr_information_div(self, width=400, height=300):
            txt = """
            <div {}>
            <h4 {}>How to interpret relationships </h4>
            <ul style='margin-top:-10px;'>
                <li>
                Positive: as variable 1 increases, so does variable 2.
                </li>
                <li>
                Negative: as variable 1 increases, variable 2 decreases.
                </li>
                <li>
                Strength: decisions can be made on the basis of strong and moderate relationships.
                </li>
                <li>
                No relationship/not significant: no statistical support for decision making.
                </li>
                 <li>
               The scatter graphs (below) are useful for visual confirmation.
                </li>
                 <li>
               The histogram (right) shows the distribution of the variable.
                </li>
            </ul>
            </div>

            """.format(self.div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

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
        table = 'crypto_daily'
        cols = list(groupby_dict.keys()) + ['crypto']
        thistab = Thistab(table,cols,[])

        # setup dates
        first_date_range = datetime.strptime("2018-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date'] - timedelta(days=2)
        first_date = last_date - timedelta(days=340)
        # initial function call
        thistab.df_load(first_date, last_date,timestamp_col='timestamp')


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
        controls_left = WidgetBox(
            datepicker_start)

        controls_right = WidgetBox(
            datepicker_end)


        # create the dashboards

        grid = gridplot([
            [thistab.notification_div['top']],
            [controls_left, controls_right],
            [thistab.title_div('Crypto families', 400)],
            [cluster_table.state],
            [thistab.notification_div['bottom']]

        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Clustering: crypto')
        return tab

    except Exception:
        logger.error('crypto:', exc_info=True)
        return tab_error_flag('Clustering: crypto')