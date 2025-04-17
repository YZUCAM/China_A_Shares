"""Code use to get all stock ticker data via tushare package
    Yi Zhu     in Cambridge             31-07-2022
    Functions: initial_tushare
                get_ticker_info
                get_trade_calendar
                iterate_get_all
                get_single_ticker_daily
"""
import time
import pandas as pd
import numpy as np
import tushare as ts
from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()

# mysql api setting
username = 'root'
password = '123456'
engine_ts = create_engine('mysql://' + username + ':' + password + '@localhost/china_a_shares')


def write_data(df):
    res = df.to_sql('stock_basic', engine_ts, index=False, if_exists='append', chunksize=500000)
    print(res)


def write_data_to_ticker_table(table_name, df):
    df.to_sql(table_name, engine_ts, index=False, if_exists='append', chunksize=5000)
    # print(table_name + ' inserted')


def read_data(sql):
    '''sql command: select table'''
    # sql = """SELECT * FROM stock_basic LIMIT 20"""
    df = pd.read_sql_query(sql, engine_ts)
    return df


def initial_tushare():
    # tushare token
    token = 'c71a16086ba6c5abc167f30933c2e1caac8806195b732b346b27e8c3'
    pro = ts.pro_api(token)
    return pro


def get_ticker_info(pro):
    # all ticker info
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,'
                                                              'list_date,market,exchange,is_hs')
    return df


def get_trade_calendar(pro, start_date, end_date):
    df = pro.trade_cal(exchange='', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')
    return df


def get_single_ticker_daily(pro, ts_code, start_date, end_date):
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return df


def get_ticker_daily_retry(pro, ts_code='', trade_date='', retry_count=3, pause=2):
    for _ in range(retry_count):
        try:
            df = pro.daily(ts_code=ts_code, trade_date=trade_date)
        except:
            time.sleep(pause)
    else:
        return df


def iterate_get_all(pro, df_trade_calendar):
    for i in df_trade_calendar['cal_date']:
        df_ticker = get_ticker_daily_retry(pro, trade_date=i)
        for j in df_ticker['ts_code']:
            write_data_to_ticker_table(j.replace('.', '').lower(), df_ticker.loc[df_ticker['ts_code'] == j])
        print(i)



if __name__ == '__main__':
    # df = read_data('SELECT * FROM stock_basic')
    # write_data(df)
    pro = initial_tushare()
    '''1st batch: 20220702, 20220731
       2nd batch: 20220101, 20220702
       3rd batch: 20190105, 20220101 took 7h   
       4th batch: 20180106, 20190105 start_time 13:30  15:50
       5th batch: 20150103, 20180106
       6th batch: 20130105, 20150103
       7th batch: 20100102, 20130105
       8th batch: 20090103, 20100102
       9th batch: 20000101, 20090103
    '''
    df_trade_calender = get_trade_calendar(pro, '20090103', '20100102')
    # df = get_single_ticker_daily('000001.SZ, 600000.SH', '20220101', '20220731')
    # df = get_ticker_daily_retry(pro, trade_date='20220729')
    iterate_get_all(pro, df_trade_calender)
    # print(df.head())
    # print(len(df))
    # print(df)
