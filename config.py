# GLOBAL VARIABLES
columns = {}
insert_sql = {}
dedup_cols = {}
create_table_sql = {}
create_indexes= {}
table_dict = {}
columns_ch = {}
load_columns = {
    'block':dict(),
    'transaction':dict(),
    'block_tx_warehouse':dict(),
}

load_columns['block']['churn'] = ['transaction_hashes', 'block_timestamp', 'miner_address',
                  'block_number','difficulty','nrg_consumed','nrg_limit',
                  'block_size','approx_nrg_reward','block_time']


columns['block'] = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "block_size", "block_timestamp","block_date", "block_year",
               "block_month", "block_day", "num_transactions",
               "block_time", "approx_nrg_reward", "transaction_hashes"]


dedup_cols['block'] = ['block_number']

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             TRANSACTIONS
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dedup_cols['transaction'] = ['transaction_hash']
columns['transaction'] = ['transaction_hash','transaction_index','block_number',
                       'transaction_timestamp','block_timestamp',"block_date",
                       'from_addr','to_addr','approx_value','nrg_consumed',
                       'nrg_price','nonce','contract_addr','transaction_year',
                       'transaction_month','transaction_day']
load_columns['transaction']['churn'] = ['block_timestamp',
                        'transaction_hash', 'from_addr',
                        'to_addr', 'approx_value', 'nrg_consumed',
                        'nrg_price']


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             block tx warehouse poolminer
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
load_columns['block_tx_warehouse']['churn'] = ['block_timestamp', 'block_number', 'to_addr',
                      'from_addr', 'miner_address', 'approx_value','transaction_hash',
                      'block_nrg_consumed','transaction_nrg_consumed',
                      'nrg_price', 'nrg_limit','block_size','block_time', 'approx_nrg_reward']

columns['block_tx_warehouse'] = ['miner_address', 'block_number','block_timestamp','block_date',
                                 'transaction_hash', 'from_addr', 'to_addr', 'approx_value']
dedup_cols['block_tx_warehouse'] = []

table_dict['block_tx_warehouse'] = {
                                'miner_address':'String',
                                'block_number' : 'UInt64',
                                'block_timestamp' : 'Datetime',
                                'block_date' : 'Date',
                                'transaction_hash': 'String',
                                'from_addr' : 'String',
                                'to_addr' : 'String',
                                'approx_value': 'Float64',
                            }