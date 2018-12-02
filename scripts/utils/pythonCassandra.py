from scripts.utils.mylogger import mylogger
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

from kafka.client import KafkaClient
from cassandra.policies import DCAwareRoundRobinPolicy
import datetime
import time
import sys
from datetime import datetime
import pprint
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

    def create_table_block(self):
        c_sql = """
                CREATE TABLE IF NOT EXISTS block (block_number bigint,
                                              miner_address varchar, miner_addr varchar,
                                              nonce varchar, difficulty bigint, 
                                              total_difficulty varchar, nrg_consumed bigint, nrg_limit bigint,
                                              block_size bigint, block_timestamp timestamp, block_month tinyint, 
                                              num_transactions bigint, block_time bigint, nrg_reward varchar, 
                                              transaction_id bigint, transaction_list varchar,
                                              PRIMARY KEY (block_number));
                 """
        self.session.execute(c_sql)
        self.session.execute("CREATE INDEX IF NOT EXISTS block_block_month_idx ON block (block_month);")
        self.session.execute("CREATE INDEX IF NOT EXISTS block_block_timestamp ON block (block_timestamp);")
        self.session.execute("""
                        CREATE INDEX IF NOT EXISTS block_miner_address_idx ON block (miner_address);
                """)
        self.session.execute("""
                        CREATE INDEX IF NOT EXISTS block_transaction_id_idx ON block (transaction_id);
                """)
        self.log.info("Block Table Created !!!")


    def create_table_transaction(self):
        c_sql = """
                CREATE TABLE IF NOT EXISTS transaction (id bigint,
                                              transaction_hash varchar, block_hash varchar, block_number bigint,
                                              transaction_index bigint, from_addr varchar, to_addr varchar, 
                                              nrg_consumed bigint, nrg_price bigint, transaction_timestamp bigint,
                                              block_timestamp timestamp, tx_value varchar, transaction_log varchar,
                                              tx_data varchar, nonce varchar, tx_error varchar, contract_addr varchar,
                                              PRIMARY KEY ((transaction_timestamp),block_number)
                                              );
                 """
        self.session.execute(c_sql)
        self.log.info("Transaction Table Created !!!")

    def insert_data_transaction(self, message):
        insert_sql = self.session.prepare(
            """ INSERT INTO transaction(
            id,transaction_hash, block_hash, block_number,
            transaction_index, from_addr, to_addr, 
            nrg_consumed, nrg_price, transaction_timestamp,
            block_timestamp, tx_value, transaction_log,
            tx_data, nonce, tx_error, contract_addr)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """
        )
        batch = BatchStatement()
        # batch.add(insert_sql, (1, 'LyubovK'))
        try:
            batch.add(insert_sql, message)
        except:
            print('#########################################################################################')
            print('#########################################################################################')
            print('message not inserted')
            print('#########################################################################################')
            print('#########################################################################################')

            self.ssc.stop()


        self.session.execute(batch)
        self.log.info('Batch Insert Completed')

    # message is a list
    def insert_data_block(self, messages):
        insert_sql = self.session.prepare("""
                                            INSERT INTO block(block_number, miner_address, 
                                            miner_addr, 
                                            nonce, difficulty, 
                                            total_difficulty, nrg_consumed, nrg_limit,
                                            block_size, block_timestamp, block_month, num_transactions,
                                            block_time, nrg_reward, transaction_id, transaction_list) 
                                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                            """)

        batch = BatchStatement()
        for message in messages:
            batch.add(insert_sql, message)
        try:
            self.session.execute(batch)
            self.log.info('Block Batch Insert Completed')
            print('#########################################################################################')
            print('#########################################################################################')
            print('#########################################################################################')
            print('#########################################################################################')
            print('SUCCESS: batch inserted into block')
            print('#########################################################################################')
            print('#########################################################################################')
            print('#########################################################################################')
            print('#########################################################################################')

        except Exception:
            logger.error('Insert error:', exc_info=True)
