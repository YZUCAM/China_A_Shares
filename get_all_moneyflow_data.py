"""Code use to get all stock ticker moneyflow data via tushare package
    Yi Zhu     in Cambridge                                 04-08-2022
    can be potentially used as modified any column and row data in all tables
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
engine_ts = create_engine('mysql://' + username + ':' + password + '@localhost/china_a_shares')


'''test 1st table new_000001sz'''


# modified to write only part of data to existed table
def write_command_to_mysql(engine, command):
    with engine.connect() as con:
        con.execute(command)
    con.close()


'''tushare API functions'''


def initial_tushare():
    # tushare token
    token = 'c71a16086ba6c5abc167f30933c2e1caac8806195b732b346b27e8c3'
    pro = ts.pro_api(token)
    return pro


def get_trade_calendar(pro, start_date, end_date):
    df = pro.trade_cal(exchange='', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')
    return df


def get_ticker_daily_moneyflow_retry(pro, ts_code='', trade_date='', retry_count=3, pause=2):
    for _ in range(retry_count):
        try:
            df = pro.moneyflow(ts_code=ts_code, trade_date=trade_date)
        except:
            time.sleep(pause)
    else:
        return df


def iterate_get_all_moneyflow(pro, df_trade_calendar):
    for i in df_trade_calendar['cal_date']:
        df = get_ticker_daily_moneyflow_retry(pro, trade_date=i)
        for j in df['ts_code']:
            target_table = j.replace('.', '').lower()
            # insert single record data to database
            f1 = 'buy_sm_vol = ' + str(int(df.loc[df['ts_code'] == j]['buy_sm_vol'])) + ', '
            f2 = 'buy_sm_amount = ' + str(float(df.loc[df['ts_code'] == j]['buy_sm_amount'])) + ', '
            f3 = 'sell_sm_vol = ' + str(int(df.loc[df['ts_code'] == j]['sell_sm_vol'])) + ', '
            f4 = 'sell_sm_amount = ' + str(float(df.loc[df['ts_code'] == j]['sell_sm_amount'])) + ', '
            f5 = 'buy_md_vol = ' + str(int(df.loc[df['ts_code'] == j]['buy_md_vol'])) + ', '
            f6 = 'buy_md_amount = ' + str(float(df.loc[df['ts_code'] == j]['buy_md_amount'])) + ', '
            f7 = 'sell_md_vol = ' + str(int(df.loc[df['ts_code'] == j]['sell_md_vol'])) + ', '
            f8 = 'sell_md_amount = ' + str(float(df.loc[df['ts_code'] == j]['sell_md_amount'])) + ', '
            f9 = 'buy_lg_vol = ' + str(int(df.loc[df['ts_code'] == j]['buy_lg_vol'])) + ', '
            f10 = 'buy_lg_amount = ' + str(float(df.loc[df['ts_code'] == j]['buy_lg_amount'])) + ', '
            f11 = 'sell_lg_vol = ' + str(int(df.loc[df['ts_code'] == j]['sell_lg_vol'])) + ', '
            f12 = 'sell_lg_amount = ' + str(float(df.loc[df['ts_code'] == j]['sell_lg_amount'])) + ', '
            f13 = 'buy_elg_vol = ' + str(int(df.loc[df['ts_code'] == j]['buy_elg_vol'])) + ', '
            f14 = 'buy_elg_amount = ' + str(float(df.loc[df['ts_code'] == j]['buy_elg_amount'])) + ', '
            f15 = 'sell_elg_vol = ' + str(int(df.loc[df['ts_code'] == j]['sell_elg_vol'])) + ', '
            f16 = 'sell_elg_amount = ' + str(float(df.loc[df['ts_code'] == j]['sell_elg_amount'])) + ', '
            f17 = 'net_mf_vol = ' + str(int(df.loc[df['ts_code'] == j]['net_mf_vol'])) + ', '
            f18 = 'net_mf_amount = ' + str(float(df.loc[df['ts_code'] == j]['net_mf_amount']))

            sql3 = 'UPDATE ' + target_table + ' SET '
            sql4 = f1 + f2 + f3 + f4 + f5 + f6 + f7 + f8 + f9 + f10 + f11 + f12 + f13 + f14 + f15 + f16 + f17 + f18
            sql5 = ' WHERE trade_date = ' + str(i)

            # print(sql3 + sql4 + sql5)

            write_command_to_mysql(engine_ts, sql3 + sql4 + sql5)

        print(i)


if __name__ == '__main__':
    write_command_to_mysql(engine_ts, 'SET SQL_SAFE_UPDATES = 0')
    pro = initial_tushare()
    # df = get_ticker_daily_moneyflow_retry(pro, trade_date='20220729')
    '''1st batch: 20220702, 20220731 start time 3:25 end time 04:08 20 days
       2nd batch: 20190105, 20220702
       3th batch: 20181201, 20190105 speed test: 2min18s, 2min10s
       4th batch: 20160102, 20181201
       5th batch: 20150103, 20160102
    '''
    df_trade_calender = get_trade_calendar(pro, '20150103', '20160102')
    iterate_get_all_moneyflow(pro, df_trade_calender)
