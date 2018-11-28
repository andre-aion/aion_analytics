from scripts.streaming.streamingDataframe import *

class Block:
    columns = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "size", "block_timestamp", 'block_month', "num_transactions",
               "block_time", "nrg_reward", "transaction_id", "transaction_list"]

    def __init__(self):
        self.df = StreamingDataframe('block', self.columns, 'block_number')

    def get_df(self):
        return self.df.get_df()

    def add_data(self, messages):
        self.df.add_data(messages)
