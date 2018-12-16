# GLOBAL VARIABLES

columns = {}
insert_sql = {}
dedup_cols = {}
create_table_sql = {}
create_indexes= {}

columns['block'] = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "size", "block_timestamp", "year", "month", "day", "num_transactions",
               "block_time", "nrg_reward", "transaction_hash", "transaction_hashes"]

dedup_cols['block'] = ['block_number']
insert_sql['block'] = """
                    INSERT INTO block(block_number, miner_address, 
                    miner_addr, 
                    nonce, difficulty, 
                    total_difficulty, nrg_consumed, nrg_limit,
                    block_size, block_timestamp, block_date, block_year, 
                    block_month, block_day, num_transactions,
                    block_time, nrg_reward, transaction_hash, transaction_hashes) 
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """

columns['transaction'] = ['transaction_hash','transaction_index','block_number'
                       'transaction_timestamp','block_timestamp',
                       'from_addr','to_addr','value','nrg_consumed',
                       'nrg_price','nonce','contract_addr','transaction_year',
                       'transaction_month','transaction_day']

create_indexes['block'] = [
        "CREATE INDEX IF NOT EXISTS block_block_year_idx ON block (block_year);",
        "CREATE INDEX IF NOT EXISTS block_block_month_idx ON block (block_month);",
        "CREATE INDEX IF NOT EXISTS block_block_day_idx ON block (block_day);",
        "CREATE INDEX IF NOT EXISTS block_block_timestamp ON block (block_timestamp);",
        "CREATE INDEX IF NOT EXISTS block_miner_address_idx ON block (miner_address);",
        "CREATE INDEX IF NOT EXISTS block_transaction_hash_idx ON block (transaction_hash);"
    ]


insert_sql['transaction'] = """ INSERT INTO transaction(
            transaction_hash,transaction_index, block_number,
            transaction_timestamp,block_timestamp, 
            from_addr, to_addr, value, 
            nrg_consumed, nrg_price, nonce, contract_addr,
            transaction_year, transaction_month, transaction_day)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

create_table_sql['transaction'] = """
                CREATE TABLE IF NOT EXISTS transaction (id bigint,
                      transaction_hash varchar, block_hash varchar, block_number bigint,
                      transaction_index bigint, from_addr varchar, to_addr varchar, 
                      nrg_consumed bigint, nrg_price bigint, transaction_timestamp bigint,
                      block_timestamp timestamp, tx_value varchar, transaction_log varchar,
                      tx_data varchar, nonce varchar, tx_error varchar, contract_addr varchar,
                      PRIMARY KEY (transaction_hash)
                      );
                """


create_table_sql['block'] = """
                CREATE TABLE IF NOT EXISTS block (block_number bigint,
                                              miner_address varchar, miner_addr varchar,
                                              nonce varchar, difficulty bigint, 
                                              total_difficulty varchar, nrg_consumed bigint, nrg_limit bigint,
                                              block_size bigint, block_timestamp timestamp, block_date timestamp, 
                                              block_year tinyint, block_month tinyint, block_day tinyint,
                                              num_transactions bigint, block_time bigint, nrg_reward varchar, 
                                              transaction_id bigint, transaction_list varchar,
                                              PRIMARY KEY (block_number));
                 """