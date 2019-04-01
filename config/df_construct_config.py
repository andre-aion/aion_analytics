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

warehouse_inputs = {'block_tx_warehouse':dict()}

load_columns['block']['models'] = ['transaction_hashes', 'block_timestamp', 'miner_address',
                  'block_number','difficulty','nrg_consumed','nrg_limit',
                  'block_size','block_time','nrg_reward']


columns['block'] = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "block_size", "block_timestamp","block_date", "block_year",
               "block_month", "block_day", "num_transactions",
               "block_time", "nrg_reward", "transaction_hashes"]


dedup_cols['block'] = ['block_number']

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             TRANSACTIONS
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dedup_cols['transaction'] = ['transaction_hash']
columns['transaction'] = ['transaction_hash','transaction_index','block_number',
                       'transaction_timestamp','block_timestamp',"block_date",
                       'from_addr','to_addr','value','nrg_consumed',
                       'nrg_price','nonce','contract_addr','transaction_year',
                       'transaction_month','transaction_day']
load_columns['transaction']['models'] = ['block_timestamp',
                        'transaction_hash', 'from_addr',
                        'to_addr', 'value', 'nrg_consumed']


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             block tx warehouse poolminer
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
load_columns['block_tx_warehouse']['models'] = ['block_timestamp', 'block_number', 'to_addr',
                      'from_addr', 'miner_address', 'value','transaction_hash',
                      'block_nrg_consumed','transaction_nrg_consumed','difficulty',
                      'nrg_limit','block_size','block_time', 'nrg_reward']

columns['block_tx_warehouse'] = ['block_number','block_timestamp','transaction_hash','miner_address',
                'total_difficulty','difficulty',
                'block_nrg_consumed','nrg_limit','num_transactions',
                'block_size','block_time','nrg_reward','block_year','block_month',
                'block_day','from_addr',
                'to_addr', 'value', 'transaction_nrg_consumed','nrg_price']

dedup_cols['block_tx_warehouse'] = []

table_dict['block_tx_warehouse'] = {
    'miner_address':'String',
    'block_number' : 'UInt64',
    'block_timestamp' : 'Datetime',
    'block_size' : 'UInt64',
    'block_time': 'UInt64',
    'difficulty': 'Float64',
    'total_difficulty':'Float64',
    'nrg_limit':'UInt64',
    'transaction_hash': 'String',
    'nrg_reward':'Float64',
    'from_addr': 'String',
    'to_addr': 'String',
    'num_transactions': 'UInt16',
    'block_nrg_consumed':'UInt64',
    'transaction_nrg_consumed': 'UInt64',
    'nrg_price': 'UInt64',
    'value': 'Float64',
    'block_year': 'UInt16',
    'block_month': 'UInt8',
    'block_day':  'UInt8'

}

warehouse_inputs['block_tx_warehouse']['block'] = ['transaction_hashes','miner_address',
                'block_number','block_timestamp','total_difficulty','difficulty',
                'nrg_consumed','nrg_limit','num_transactions',
                'block_size','block_time','nrg_reward','year','month','day']


warehouse_inputs['block_tx_warehouse']['transaction'] = [
                'transaction_hash', 'from_addr',
                'to_addr', 'value', 'nrg_consumed','nrg_price']



# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             block tx warehouse poolminer
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
table_dict['checkpoint'] = {
    'table':'String',
    'column':'String',
    'offset':'String',
    'timestamp':'Datetime'
}

columns['checkpoint'] = ['table','column','offset','timestamp']

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# WHEN JOINED, WHEN CHURNED
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
table_dict['account_activity_churn'] = {
    'block_timestamp': 'Date',
    'new_lst': 'String',
    'churned_lst':'String',
    'retained_lst':'String',
    'active_lst':'String',
    'new': 'UInt64',
    'churned':'UInt64',
    'retained':'UInt64',
    'active':'UInt64',
    'value': 'Float64',
    'value_counts':'UInt64',
    'block_size': 'Float64',
    'block_time': 'Float64',
    'difficulty': 'Float64',
    'nrg_limit': 'Float64',
    'nrg_reward': 'Float64',
    'num_transactions': 'Float64',
    'block_nrg_consumed': 'Float64',
    'transaction_nrg_consumed': 'Float64',
    'nrg_price': 'Float64',
    'block_year': 'UInt16',
    'block_month': 'UInt16',
    'block_day':'UInt16',
    'day_of_week':'String',

}

table_dict['account_activity'] = {
    'activity':'String',
    'address': 'String',
    'block_day': 'UInt8',
    'block_hour': 'UInt8',
    'block_month': 'UInt8',
    'block_number': 'UInt64',
    'block_timestamp': 'Datetime',
    'block_year': 'UInt16',
    'day_of_week': 'String',
    'from_addr':'String',
    'to_addr': 'String',
    'transaction_hash':'String',
    'value': 'Float64',

}

table_dict['account_ext_warehouse'] = {
    'block_timestamp': 'Datetime',
    'address': 'String',
    'timestamp_of_first_event': 'Datetime',
    'update_type':'String',
    'account_type': 'String',
    'status':'String',
    'amount': 'Float64',
    'transaction_cost': 'Float64',
    'block_time': 'Float64',
    'balance':'Float64',
    'difficulty': 'Float64',
    'mining_reward': 'Float64',
    'nrg_reward': 'Float64',
    'num_transactions': 'Float64',
    'hash_power': 'Float64',
    'year':'UInt16',
    'month':'UInt16',
    'day':'UInt16',
    'hour':'UInt16'

}
################################################################
#            MODEL FUNCTION
#################################################################
'''
def model(self, date):
    try:
        pass
    except Exception:
        logger.error('', exc_info=True)

'''