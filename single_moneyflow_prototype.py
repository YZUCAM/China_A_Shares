"""Code use to get single ticker money flow data and write into database
    prototype
    Potentially can be used to append any columns into database
    Yi Zhu     in Cambridge                  05-08-2022
"""


import time
import pandas as pd
import numpy as np
import tushare as ts
from sqlalchemy import create_engine, text
import pymysql

pymysql.install_as_MySQLdb()

# mysql api setting
username = 'root'
password = '123456'
database = 'china_a_shares'
engine_ts = create_engine('mysql://' + username + ':' + password + '@localhost/' + database)


# def sql_command(command):
#     sql_text = text(command)
#     return sql_text

def write_command_to_mysql(engine, command):
    with engine.connect() as con:
        con.execute(command)
    con.close()


def query_command_to_mysql(engine, command):
    with engine.connect() as con:
        rs = con.execute(command)
        for row in rs:
            print(row)
    con.close()


def read_data(sql):
    '''sql command: select table'''
    # sql = """SELECT * FROM stock_basic LIMIT 20"""
    df = pd.read_sql_query(sql, engine_ts)
    return df


'''tushare API functions'''


def initial_tushare():
    # tushare token
    token = 'c71a16086ba6c5abc167f30933c2e1caac8806195b732b346b27e8c3'
    pro = ts.pro_api(token)
    return pro


# get single ticker
def get_ticker_daily_moneyflow_retry(pro, ts_code='', trade_date='', retry_count=3, pause=2):
    for _ in range(retry_count):
        try:
            df = pro.moneyflow(ts_code=ts_code, trade_date=trade_date)
        except:
            time.sleep(pause)
    else:
        return df


if __name__ == '__main__':
    sql = """SHOW tables"""
    df = read_data(sql)
    df_list = df.values.tolist()
    # for i in df_list:
    #     print(''.join(i))
    target_table = str(''.join(df_list[-2]))
    print(target_table)


    # insert new column to table
    # sql2 = 'ALTER TABLE ' + target_table + ' ADD buy_sm_vol INT AFTER amount, ' \
    #                                        'ADD buy_sm_amount FLOAT AFTER buy_sm_vol, ' \
    #                                        'ADD sell_sm_vol INT AFTER buy_sm_amount, ' \
    #                                        'ADD sell_sm_amount FLOAT AFTER sell_sm_vol, ' \
    #                                        'ADD buy_md_vol INT AFTER sell_sm_amount, ' \
    #                                        'ADD buy_md_amount FLOAT AFTER buy_md_vol, ' \
    #                                        'ADD sell_md_vol INT AFTER buy_md_amount, ' \
    #                                        'ADD sell_md_amount FLOAT AFTER sell_md_vol, ' \
    #                                        'ADD buy_lg_vol INT AFTER sell_md_amount, ' \
    #                                        'ADD buy_lg_amount FLOAT AFTER buy_lg_vol, ' \
    #                                        'ADD sell_lg_vol INT AFTER buy_lg_amount, ' \
    #                                        'ADD sell_lg_amount FLOAT AFTER sell_lg_vol, ' \
    #                                        'ADD buy_elg_vol INT AFTER sell_lg_amount, ' \
    #                                        'ADD buy_elg_amount FLOAT AFTER buy_elg_vol, ' \
    #                                        'ADD sell_elg_vol INT AFTER buy_elg_amount, ' \
    #                                        'ADD sell_elg_amount FLOAT AFTER sell_elg_vol, ' \
    #                                        'ADD net_mf_vol FLOAT AFTER sell_elg_amount, ' \
    #                                        'ADD net_mf_amount FLOAT AFTER net_mf_vol'
    # print(sql2)
    #
    # write_command_to_mysql(engine_ts, sql2)


    # get money flow data
    pro = initial_tushare()
    df = get_ticker_daily_moneyflow_retry(pro, ts_code='000001.SZ', trade_date='20220729')
    print(df)


    # insert single record data to database
    f1 = 'buy_sm_vol = ' + str(int(df.loc[df['ts_code'] == '000001.SZ']['buy_sm_vol'])) + ', '
    f2 = 'buy_sm_amount = ' + str(float(df.loc[df['ts_code'] == '000001.SZ']['buy_sm_amount'])) + ', '
    f3 = 'sell_sm_vol = ' + str(int(df.loc[df['ts_code'] == '000001.SZ']['sell_sm_vol'])) + ', '
    f4 = 'sell_sm_amount = ' + str(float(df.loc[df['ts_code'] == '000001.SZ']['sell_sm_amount'])) + ', '
    f5 = 'buy_md_vol = ' + str(int(df.loc[df['ts_code'] == '000001.SZ']['buy_md_vol'])) + ', '
    f6 = 'buy_md_amount = ' + str(float(df.loc[df['ts_code'] == '000001.SZ']['buy_md_amount'])) + ', '
    f7 = 'sell_md_vol = ' + str(int(df.loc[df['ts_code'] == '000001.SZ']['sell_md_vol'])) + ', '
    f8 = 'sell_md_amount = ' + str(float(df.loc[df['ts_code'] == '000001.SZ']['sell_md_amount'])) + ', '
    f9 = 'buy_lg_vol = ' + str(int(df.loc[df['ts_code'] == '000001.SZ']['buy_lg_vol'])) + ', '
    f10 = 'buy_lg_amount = ' + str(float(df.loc[df['ts_code'] == '000001.SZ']['buy_lg_amount'])) + ', '
    f11 = 'sell_lg_vol = ' + str(int(df.loc[df['ts_code'] == '000001.SZ']['sell_lg_vol'])) + ', '
    f12 = 'sell_lg_amount = ' + str(float(df.loc[df['ts_code'] == '000001.SZ']['sell_lg_amount'])) + ', '
    f13 = 'buy_elg_vol = ' + str(int(df.loc[df['ts_code'] == '000001.SZ']['buy_elg_vol'])) + ', '
    f14 = 'buy_elg_amount = ' + str(float(df.loc[df['ts_code'] == '000001.SZ']['buy_elg_amount'])) + ', '
    f15 = 'sell_elg_vol = ' + str(int(df.loc[df['ts_code'] == '000001.SZ']['sell_elg_vol'])) + ', '
    f16 = 'sell_elg_amount = ' + str(float(df.loc[df['ts_code'] == '000001.SZ']['sell_elg_amount'])) + ', '
    f17 = 'net_mf_vol = ' + str(int(df.loc[df['ts_code'] == '000001.SZ']['net_mf_vol'])) + ', '
    f18 = 'net_mf_amount = ' + str(float(df.loc[df['ts_code'] == '000001.SZ']['net_mf_amount']))

    sql3 = 'UPDATE ' + target_table + ' SET '
    sql4 = f1 + f2 + f3 + f4 + f5 + f6 + f7 + f8 + f9 + f10 + f11 + f12 + f13 + f14 + f15 + f16 + f17 + f18
    sql5 = ' WHERE trade_date = 20220729'


    print(sql3 + sql4 + sql5)

    write_command_to_mysql(engine_ts, sql3 + sql4 + sql5)

