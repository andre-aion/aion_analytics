import pandas as pd
import dask as dd
from dask.dataframe import from_pandas,from_array
import gc

# Manage dask dataframes from streaming library
class StreamingDataframe:
    def __init__(self, table_name, columns, dedup_columns):
        self.partitions = 15
        self.table_name = table_name
        self.columns = columns
        self.dedup_columns = dedup_columns
        # initialize as pandas
        df = pd.DataFrame(columns=columns)
        # convert to dask
        self.df = from_pandas(df, npartitions=15, name=table_name, sort=True)

    # data: dict of lists from rdds
    def add_data(self, data, chunksize=500):
        # create temp dataframe from dict
        df = pd.DataFrame.from_dict(data)
        df_temp = from_pandas(df, npartitions=15,sort=True)
        # append to current array
        try:
            self.df = self.df.append(df_temp)
        except Exception as ex:
            print("DASK ARRAY CONCATENATION PROBLEM WITH {}: {}"
                  .format(self.table_name,ex))
        self.deduplicate()
        del df
        del data
        gc.collect()


    # drop duplicates by values in columns
    def deduplicate(self):
        try:
            self.df.drop_duplicates(subset=self.dedup_columns, keep='last', inplace=True)
        except Exception as ex:
            print("DEDUPLICATON ERROR WITH {} : {}".format(self.table_name, ex))


    # truncate data frame
    # position : top or bottom
    def truncate_data(self,numrows, row_indices_list, column='block_timestamp'):
        self.df.drop(row_indices_list, inplace=True)

    def get_df(self):
        return self.df

