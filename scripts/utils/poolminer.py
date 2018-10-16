import pandas as pd

#import helper functions
from scripts.utils.utils import optimize_dataframe


def munge_blockdetails(df_blockdetails,df_poolinfo):
    # join tables
    df_blockdetails = optimize_dataframe(df_blockdetails, 'timestamp')
    df_blockdetails['addr'] = df_blockdetails['address'].str[0:10]
    df_blockdetails['date'] = df_blockdetails['timestamp'].dt.date
    # df_blockdetails = pd.merge(df_blockdetails, df_poolinfo, left_on='address',right_on='address',how='inner',suffixes=('', '_y'))
    # df_blockdetails.drop(['Unnamed: 0'], inplace=True)
    return df_blockdetails