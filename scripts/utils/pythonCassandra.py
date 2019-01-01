import datetime
from time import mktime

from scripts.utils.mylogger import mylogger
from config import insert_sql, create_table_sql, create_indexes
from concurrent.futures import ThreadPoolExecutor

import logging
from cassandra.cluster import Cluster, BatchStatement
import pandas as pd
import dask as dd
from datetime import datetime
import gc
from pdb import set_trace

executor = ThreadPoolExecutor(max_workers=20)

logger = mylogger(__file__)


class PythonCassandra:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.keyspace = None
        self.log = None

    def __del__(self):
        self.cluster.shutdown()

    def createsession(self):
        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect(self.keyspace)

    def getsession(self):
        return self.session

    # How about Adding some logs info to see what went wrong
    def setlogger(self):
        log = logging.getLogger()
        log.setLevel('INFO')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        log.addHandler(handler)
        self.log = log

    # Create Keyspace based on Given Name
    def createkeyspace(self, keyspace):
        """
        :param keyspace:  The Name of Keyspace to be created
        :return:
        """
        # Before we create new lets check if exiting keyspace; we will drop that and create new
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        keyspace_exists = False
        if keyspace in [row[0] for row in rows]:
            keyspace_exists = True
        if keyspace_exists is False:
            # self.logs.info("dropping existing keyspace...")
            # self.session.execute("DROP KEYSPACE " + keyspace)
            self.log.info("creating keyspace...")
            self.session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
                """ % keyspace)
            self.log.info("setting keyspace...")
        self.session.set_keyspace(keyspace)

    def create_table(self, table):
        c_sql = create_table_sql[table]
        self.session.execute(c_sql)

        # create indexes
        for sql in create_indexes[table]:
            self.session.execute(sql)

            logger.warning("Attempted:%s", sql)

        #logger.warning("%s Table & indexes Created/Checked !!!", table)


    def insert_data(self, table, messages):
        qry = self.session.prepare(insert_sql[table])
        batch = BatchStatement()
        # batch.add(insert_sql, (1, 'LyubovK'))
        for message in messages:
            #logger.warning("insert %s data:%s",table,message)
            try:
                batch.add(qry, message)
            except Exception:
                logger.error(table.upper()+' INSERT FAILED:', exc_info=True)

        self.session.execute(batch)

    # cols are a list
    def construct_read_query(self, table, cols, startdate, enddate):
        qry = 'select '
        if len(cols) >= 1:
            for pos, col in enumerate(cols):
                if pos > 0:
                    qry += ','
                qry += col
        else:
            qry += '*'

        qry += """ from {} where block_timestamp >={} and 
            block_timestamp <={} ALLOW FILTERING""" \
            .format(table, startdate, enddate)

        logger.warning('query:%s', qry)
        return qry

    # dates  are in milliseconds from sliders
    def load_from_daterange(self, table, cols, from_date, to_date):
        logger.warning('cass load from_date:%s', from_date)
        logger.warning('cass load to_date:%s', to_date)
        #date sanity check
        if from_date > to_date:
            logger.warning("END DATE IS GREATER THAN START DATE")
            logger.warning("BOTH DATES SET TO START DATE")
            from_date = to_date

        try:
            if isinstance(from_date, int) == True:
                # convert ms from slider to nano for cassandra
                if from_date < 16307632000:
                    from_date = from_date * 1000
                    to_date = to_date * 1000
            elif isinstance(from_date, str) == True:
                # convert from datetime to ns
                from_date = self.date_to_cass_ts(from_date)
                to_date = self.date_to_cass_ts(to_date)

            # construct query
            qry = self.construct_read_query(table, cols,
                                       from_date,
                                       to_date)
            df = pd.DataFrame(list(self.session.execute(qry)))
            df = dd.dataframe.from_pandas(df, npartitions=15)
            #logger.warning('data loaded from daterange :%s', df.tail(5))
            return df

        except Exception:
            logger.error('cass load from daterange:%s', exc_info=True)

    # convert date format for building cassandra queries
    def date_to_cass_ts(self, ts):
        if isinstance(ts, str):
            ts = datetime.strptime(ts, '%Y-%m-%d')
            ts = int(ts.timestamp() * 1000)
        elif isinstance(ts, datetime):
            # ts = pd.Timestamp(ts, unit='ns')
            ts = int(mktime(ts.timetuple()) * 1000)
        logger.warning('date_to_cass_ts:%s', ts)
        return ts
