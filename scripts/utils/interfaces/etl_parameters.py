from pandas.io.json import json_normalize

from scripts.databases.pythonMongo import PythonMongo
from scripts.utils.mylogger import mylogger
from datetime import datetime
logger = mylogger(__file__)


class EtlParams:
    def __init__(self):
        self.pym = PythonMongo("aion")
        self.df = None

    def load_df(self,table='etl_parameter'):
        df = json_normalize(list(self.pym.db[table].find({})))
        logger.warning('df:%s',df)
        return df

    def update_parameters(self,etl='twitter'):
        try:
            # filter
            etl_params = self.load_df()
            etl_params = etl_params.drop('_id', axis=1)
            etl_df = self.load_df(table='etl')
            etl_df = etl_df.rename(index=str,columns={'etl':'name'})
            # join
            etl_params = etl_params.merge(etl_df,left_on='etl',right_on='_id',how='left')
            df = etl_params[etl_params.name == etl]

            if df is not None:
                if self.df is not None:
                    if self.df.equals(df):
                        if len(df) > 0:
                            self.df = df
                            return df
                    else:
                        return None
                else:
                    if len(df) > 0:
                        self.df = df
                        return df
            return None

        except Exception:
            logger.error('get params',exc_info=True)


    def update_items(self,df,items):
        try:
            if df is not None:
                items_dct = dict(zip(df.label.tolist(), df.handle.tolist()))
                if len(df) > 0:
                    for idx,key in enumerate(items_dct.keys()):
                        item = key.replace('-','_').lower()
                        item = item.replace(' ','_')
                        #self.rename_dict[item] = items_dct[key]
                        if item not in items:
                            items.append(item)

                    items = sorted(list(set(items)))
            return items

        except Exception:
            logger.error('update params', exc_info=True)


    def load_items(self,table='external_daily'):
        try:
            df = json_normalize(list(self.pym.db[table].find({},{'_id':False})))
        except Exception:
            logger.error('update params', exc_info=True)

