import pandas as pd
import pdb
#import helper functions
from scripts.utils.utils import optimize_dataframe


def munge_blockdetails(df_blockdetails):
    # join tables
    df_blockdetails['addr'] = df_blockdetails['miner_address'].str[0:10]
    # df_blockdetails = pd.merge(df_blockdetails, df_poolinfo, left_on='address',right_on='address',how='inner',suffixes=('', '_y'))
    # df_blockdetails.drop(['Unnamed: 0'], inplace=True)
    return df_blockdetails