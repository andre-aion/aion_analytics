from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)

def str_to_hex(x):
    logger.warning("the str to convert:%s", x)
    if isinstance(x,int):
        return x
    elif isinstance(x,str):
        return int(x, base=16)
    else:
        return 0

def calc_hashrate(df,blockcount):
    logger.warning('STARTING HASHRATE CALCULATIONS')
    df['rolling_mean'] = df.block_time.rolling(blockcount).mean()
    df.compute()
    df = df.dropna()
    df['hashrate'] = df.difficulty / df.rolling_mean
    return df

