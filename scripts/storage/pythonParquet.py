from datetime import datetime, date
from enum import Enum
from os.path import join, dirname

import pandas as pd
from tornado.gen import coroutine

from scripts.utils.mylogger import mylogger
import dask as dd
import glob
logger = mylogger(__file__)
engine = 'fastparquet'

class PythonParquet:
    def  __init__(self):
       pass

    #params is a list
    def set_filepath(self,params=[]):
        to_append = ''
        for param in params:
            to_append += param

        return join(dirname(__file__),
                    "../../data/parquet/",
                    to_append
                    )

    def date_to_str(self,ts):
        if isinstance(ts,datetime):
            return datetime.strftime(ts,"%Y-%m-%d")
        elif isinstance(ts, date):
            return date.strftime(ts,"%Y-%m-%d")
        elif isinstance(ts, str):
            return ts[0:10]

    def str_to_date(self,ts):
        return pd.to_datetime(ts, infer_datetime_format=True)

    def str_to_datetime(self,ts, format="%Y-%m-%d"):
        return datetime.strptime(ts, format)


    # key_params: list of parameters to put in key
    def compose_key(self, key_params, start_date, end_date):
        if isinstance(key_params, str):
            key_params = key_params.split(',')
        start_date = self.date_to_str(start_date)
        end_date = self.date_to_str(end_date)
        logger.warning('start_date in compose key:%s', start_date)
        key = ''
        for kp in key_params:
            if not isinstance(kp, str):
                kp = str(kp)
            key += kp + ':'
        key = '{}{}:{}'.format(key, start_date, end_date)
        return key

    @coroutine
    def save(self,df,key_params,start_date,end_date):
        try:
            start_date = self.date_to_str(start_date)
            end_date = self.date_to_str(end_date)

            key = self.compose_key(key_params,start_date,end_date)
            savepath = self.set_filepath(key)
            logger.warning("filepath to save:%s", savepath)


            # avoid overwriting
            savdir = self.set_filepath(['*'])
            key_lst = self.get_list_of_files(savdir)
            key_exists = False
            for path in key_lst:
                lst = path.split('/')
                logger.warning("search key in save:%s",lst[-1])
                if key == lst[-1]:
                    key_exists = True
                    break

            if not key_exists:
                df.to_parquet(savepath, engine=engine, compression='gzip')
                logger.warning("SAVED TO PARQUET:%s",key_params)

        except Exception:
            logger.error("Save:",exc_info=True)

    def get_list_of_files(self,dir):
        return glob.glob(dir)


    def load(self,key_params,req_start_date,req_end_date):
        try:
            df = None
            search_params = ''
            logger.warning("in load,key_params:%s",key_params)
            for kp in key_params:
                search_params += kp +':'

            path = self.set_filepath([search_params,'*'])
            # get list of filenames in folder
            keylst = self.get_list_of_files(path)
            logger.warning("LIST OF PARQUET:%s", keylst)

            for key in keylst:
                # split key into list to compare dates
                lst = key.split(":")
                logger.warning("lst time:%s",lst[-2])
                logger.warning("req time:%s",req_start_date)

                stored_start_date = self.str_to_datetime(lst[-2])
                stored_end_date = self.str_to_datetime(lst[-1])
                if isinstance(req_start_date,int):
                    req_start_date = datetime.fromtimestamp(req_start_date)
                    req_end_date = datetime.fromtimestamp(req_end_date)

                logger.warning("after, lst time:%s", lst[-2])
                logger.warning("after, req time:%s", req_start_date)

                # get first dataframe that overlaps the required dataframe
                if req_start_date >= stored_start_date and \
                    req_end_date <= stored_end_date:
                    path = self.set_filepath([key])
                    df = dd.dataframe.read_parquet(path,engine=engine)

                    df = df.repartition(npartitions=100)
                    df = df.dropna(subset=['block_date', 'block_timestamp'])

                    logger.warning("filepath in load:%s", path)
                    logger.warning("LOADED IN PARQUET:%s", df.head(10))

                    break

            return df
        except Exception:
            logger.error("Load:",exc_info=True)
            return None

