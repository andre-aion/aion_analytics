from scripts.streaming.streamingDataframe import *
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)


class Block:
    """
    columns = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "size", "block_timestamp", "year", "month", "day", "num_transactions",
               "block_time", "nrg_reward", "transaction_hash", "transaction_hashes"]
    """

    columns = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "size", "block_timestamp", 'block_month', "num_transactions",
               "block_time", "nrg_reward", "transaction_id", "transaction_list"]

    def __init__(self):
        self.df = StreamingDataframe('block', self.columns, ['block_number'])

    def get_df(self):
        try:
            return self.df.get_df()
        except Exception:
            logger.error('get block dataframe:', exc_info=True)
        return

    def add_data(self, messages):
        self.df.add_data(messages)
