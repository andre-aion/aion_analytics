from pandas.api.types import is_string_dtype
from pdb import set_trace


def str_to_hex(x):
    return int(x, base=16)


def calc_hashrate(df,blockcount):
    df['rolling_mean'] = df['block_time'].rolling(blockcount).mean()
    df = df.dropna()
    #if string conver to int
    if is_string_dtype(df['difficulty']):
        df['difficulty'] = df['difficulty'].apply(str_to_hex)
    df['hashrate'] = df.difficulty / df.rolling_mean
    return df