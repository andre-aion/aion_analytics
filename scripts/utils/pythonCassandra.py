from scripts.utils.mylogger import mylogger
from config import insert_sql, create_table_sql, create_indexes
import argparse
import logging
import os
import json
import mysql
from mysql.connector import errorcode

import logging
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

import gc
from pdb import set_trace

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

    def create_table_block(self, table):
        c_sql = create_table_sql[table]
        self.session.execute(c_sql)
        """
        self.session.execute("CREATE INDEX IF NOT EXISTS block_block_year_idx ON block (block_year);")
        self.session.execute("CREATE INDEX IF NOT EXISTS block_block_month_idx ON block (block_month);")
        self.session.execute("CREATE INDEX IF NOT EXISTS block_block_day_idx ON block (block_day);")

        self.session.execute("CREATE INDEX IF NOT EXISTS block_block_timestamp ON block (block_timestamp);")
        self.session.execute("CREATE INDEX IF NOT EXISTS block_miner_address_idx ON block (miner_address);"
        self.session.execute("CREATE INDEX IF NOT EXISTS block_transaction_hash_idx ON block (transaction_hash);"
        """
        # create indexes
        for sql in create_indexes[table]:
            self.session.execute(sql)

        logger.warning("%s Table Created !!!",table)



    def insert_data(self, table, message):
        qry = self.session.prepare(insert_sql[table])
        batch = BatchStatement()
        # batch.add(insert_sql, (1, 'LyubovK'))
        try:
            batch.add(insert_sql, message)
        except Exception:
            logger.error(table+' insert failed:', exc_info=True)
            self.ssc.stop()


        self.session.execute(batch)
        self.log.info('Batch Insert Completed')

