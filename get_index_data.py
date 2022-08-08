"""Code use to get china A shares index data via tushare package
    Yi Zhu     in Cambridge                         07-08-2022
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
password = '4513854'
engine_ts = create_engine('mysql://' + username + ':' + password + '@localhost/china_a_shares_index')


def write_data(df):
    res = df.to_sql('stock_basic', engine_ts, index=False, if_exists='append', chunksize=500000)
    print(res)


def write_data_to_ticker_table(table_name, df):
    df.to_sql(table_name, engine_ts, index=False, if_exists='append', chunksize=5000)
    # print(table_name + ' inserted')


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


def get_index_data(pro, ts_code, start_date='', end_date=''):
    df = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return df


if __name__ == '__main__':
    pro = initial_tushare()
    ts_code = {'上证指数': '000001.SH', '沪深300': '000300.SH', '上证A指': '000002.SH', '上证B指': '000003.SH',
               '深证成指': '399001.SZ', '创业板指': '399006.SZ', '深证100': '399330.SZ', '中小板指': '399005.SZ'}
    df_index = get_index_data(pro, ts_code=ts_code['上证指数'])
    print(df_index.head())
    # write_data_to_ticker_table('000001sh', df_index)
    # write_data_to_ticker_table('399001sz', df_index)
