"""Code use to get add more columns into mysql database
    Yi Zhu     in Cambridge                  04-08-2022
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
password = '4513854'
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


if __name__ == '__main__':
    sql = """SHOW tables"""
    df = read_data(sql)
    df_list = df.values.tolist()
    # for i in df_list:
    #     print(''.join(i))
    for i in df_list[:-2]:
        target_table = str(''.join(i))
        # print(target_table)

        # construct command
        # insert new column to table
        sql2 = 'ALTER TABLE ' + target_table + ' ADD buy_sm_vol INT AFTER amount, ' \
                                               'ADD buy_sm_amount FLOAT AFTER buy_sm_vol, ' \
                                               'ADD sell_sm_vol INT AFTER buy_sm_amount, ' \
                                               'ADD sell_sm_amount FLOAT AFTER sell_sm_vol, ' \
                                               'ADD buy_md_vol INT AFTER sell_sm_amount, ' \
                                               'ADD buy_md_amount FLOAT AFTER buy_md_vol, ' \
                                               'ADD sell_md_vol INT AFTER buy_md_amount, ' \
                                               'ADD sell_md_amount FLOAT AFTER sell_md_vol, ' \
                                               'ADD buy_lg_vol INT AFTER sell_md_amount, ' \
                                               'ADD buy_lg_amount FLOAT AFTER buy_lg_vol, ' \
                                               'ADD sell_lg_vol INT AFTER buy_lg_amount, ' \
                                               'ADD sell_lg_amount FLOAT AFTER sell_lg_vol, ' \
                                               'ADD buy_elg_vol INT AFTER sell_lg_amount, ' \
                                               'ADD buy_elg_amount FLOAT AFTER buy_elg_vol, ' \
                                               'ADD sell_elg_vol INT AFTER buy_elg_amount, ' \
                                               'ADD sell_elg_amount FLOAT AFTER sell_elg_vol, ' \
                                               'ADD net_mf_vol FLOAT AFTER sell_elg_amount, ' \
                                               'ADD net_mf_amount FLOAT AFTER net_mf_vol'
        # print(sql2)

        write_command_to_mysql(engine_ts, sql2)
        print(target_table + ' has been altered.')

