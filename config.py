# GLOBAL VARIABLES
columns = {}
insert_sql = {}
dedup_cols = {}
create_table_sql = {}
create_indexes= {}

columns['block'] = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "block_size", "block_timestamp","block_date", "year", "month", "day", "num_transactions",
               "block_time", "nrg_reward", "transaction_hashes"]

dedup_cols['block'] = ['block_number']
insert_sql['block'] = """
                    INSERT INTO block(block_number, miner_address, 
                    miner_addr, nonce, difficulty, 
                    total_difficulty, nrg_consumed, nrg_limit,
                    block_size, block_timestamp, block_date, block_year, 
                    block_month, block_day, num_transactions,
                    block_time, nrg_reward, transaction_hashes) 
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """


create_table_sql['block'] = """
                CREATE TABLE IF NOT EXISTS block (block_number bigint,
                                              miner_address varchar, miner_addr varchar,
                                              nonce varchar, difficulty bigint, 
                                              total_difficulty bigint, nrg_consumed bigint, nrg_limit bigint,
                                              block_size bigint, block_timestamp timestamp, block_date timestamp, 
                                              block_year smallint, block_month tinyint, block_day tinyint,
                                              num_transactions smallint, block_time int, nrg_reward varchar , 
                                              transaction_hashes varchar,
                                              PRIMARY KEY (block_number));
                 """
create_indexes['block'] = [
        "CREATE INDEX IF NOT EXISTS block_block_year_idx ON block (block_year);",
        "CREATE INDEX IF NOT EXISTS block_block_month_idx ON block (block_month);",
        "CREATE INDEX IF NOT EXISTS block_block_day_idx ON block (block_day);",
        "CREATE INDEX IF NOT EXISTS block_block_date_idx ON block (block_date);",
        "CREATE INDEX IF NOT EXISTS block_miner_address_idx ON block (miner_address);"
    ]

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             TRANSACTIONS
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dedup_cols['transaction'] = ['transaction_hash']
columns['transaction'] = ['transaction_hash','transaction_index','block_number',
                       'transaction_timestamp','block_timestamp',"block_date",
                       'from_addr','to_addr','value','nrg_consumed',
                       'nrg_price','nonce','contract_addr','transaction_year',
                       'transaction_month','transaction_day']


insert_sql['transaction'] = """ INSERT INTO transaction(
            transaction_hash,transaction_index, block_number,
            transaction_timestamp,block_timestamp, block_date, 
            from_addr, to_addr, value, 
            nrg_consumed, nrg_price, nonce, contract_addr,
            transaction_year, transaction_month, transaction_day)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

create_table_sql['transaction'] = """
                CREATE TABLE IF NOT EXISTS transaction (
                      transaction_hash varchar,transaction_index smallint, block_number bigint,
                      transaction_timestamp bigint,block_timestamp timestamp, block_date timestamp,
                      from_addr varchar, to_addr varchar, tx_value varchar,
                      nrg_consumed bigint, nrg_price bigint, nonce varchar, contract_addr varchar,
                      transaction_year smallint, transaction_month tinyint, transaction_day tinyint,
                      PRIMARY KEY (block_number, transaction_index)
                      ) WITH CLUSTERING ORDER BY (transaction_index ASC);
                """

create_indexes['transaction'] = [
        "CREATE INDEX IF NOT EXISTS transaction_transaction_year_idx ON transaction (transaction_year);",
        "CREATE INDEX IF NOT EXISTS transaction_transaction_month_idx ON transaction (transaction_month);",
        "CREATE INDEX IF NOT EXISTS transaction_transaction_day_idx ON transaction (transaction_day);",
        "CREATE INDEX IF NOT EXISTS transaction_block_date_idx ON transaction (block_date);",
        "CREATE INDEX IF NOT EXISTS transaction_transaction_timestamp_idx ON transaction (transaction_timestamp);",
        "CREATE INDEX IF NOT EXISTS transaction_from_addr_idx ON transaction (from_addr);",
        "CREATE INDEX IF NOT EXISTS transaction_to_addr_idx ON transaction (to_addr);",
        "CREATE INDEX IF NOT EXISTS transaction_contract_addr_idx ON transaction (contract_addr);"
]

