# GLOBAL VARIABLES
columns = {}
insert_sql = {}
dedup_cols = {}
create_table_sql = {}
create_indexes= {}


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
                       'from_addr','to_addr','value','nrg_consumed',
                       'nrg_price','nonce','contract_addr','year',
                       'month','day']


