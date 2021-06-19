# coding:utf-8
import pymysql
from sqlalchemy import create_engine
import pandas as pd

from logger.logger import *
from core.datas.ts_requests.ts_requests import ts_requests


class TsInfo(object):

    ts_code_dict = dict()


class SQLOper(object):

    host = 'localhost'
    port = 3306
    user = 'root'
    passwd = '123456'
    db = 'tushare'
    connect_timeout = 1

    engine = create_engine(f'mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}')

    def execute_sql(self, sql):
        conn = pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               passwd=self.passwd,
                               db=self.db,
                               connect_timeout=self.connect_timeout)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        conn.commit()
        cursor.close()
        conn.close()
        return results

    def execute_many_sql(self, sql_list):
        conn = pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               passwd=self.passwd,
                               db=self.db,
                               connect_timeout=self.connect_timeout)
        cursor = conn.cursor()
        results = list()
        for sql in sql_list:
            cursor.execute(sql)
            results.append(cursor.fetchall())
        conn.commit()
        cursor.close()
        conn.close()
        return results

    def create_ts_daily_date_table(self):
        create_config_date_table_sql = (
            "CREATE TABLE IF NOT EXISTS config("
            "variable_name VARCHAR(128),"
            "value VARCHAR(128),"
            "set_time timestamp,"
            "UNIQUE KEY variable_name_unique(variable_name)"
            ");"
        )
        self.execute_sql(create_config_date_table_sql)
        #       ['证券代码', '日期', '开盘价', '最高价', '最低价', '收盘价', '前收盘价', '涨跌额', '涨跌幅',
        #        '成交量', '成交额', '换手率', '换手率(自由流通股)', '量比', '市盈率', '滚动市盈率',
        #        '市净率', '市销率', '滚动市销率', '股息率', '滚动股息率', '总股本', '流通股本',
        #        '自由流通股本', '总市值', '流通市值']
        create_ts_daily_date_table_sql = (
            "CREATE TABLE IF NOT EXISTS ts_daily("
            "ts_id int PRIMARY KEY AUTO_INCREMENT,"
            "ts_trade_date DATE,"
            "ts_code VARCHAR(20),"
            "ts_name VARCHAR(20),"
            "ts_open float,"
            "ts_high float,"
            "ts_low float,"
            "ts_close float,"
            "ts_pre_close float,"
            "ts_change float,"
            "ts_pct_chg float,"
            "ts_vol float,"
            "ts_amount float,"
            "ts_turnover_rate float,"
            "ts_turnover_rate_f float,"
            "ts_volume_ratio float,"
            "ts_pe float,"
            "ts_pe_ttm float,"
            "ts_pb float,"
            "ts_ps float,"
            "ts_ps_ttm float,"
            "ts_dv_ratio float,"
            "ts_dv_ttm float,"
            "ts_total_share float,"
            "ts_float_share float,"
            "ts_free_share float,"
            "ts_total_mv float,"
            "ts_circ_mv float,"
            "UNIQUE KEY ts_code_trade_date(ts_code, ts_trade_date), "
            "INDEX ts_code(ts_code), "
            "INDEX ts_trade_date(ts_trade_date)"
            ");")
        self.execute_sql(create_ts_daily_date_table_sql)


class TsMySQL(SQLOper, TsInfo):

    def add_ts_daily(self, df):
        sql_df = df.rename(columns={
            '日期': 'ts_trade_date',
            '证券代码': 'ts_code',
            '证券名称': 'ts_name',
            '开盘价': 'ts_open',
            '最高价': 'ts_high',
            '最低价': 'ts_low',
            '收盘价': 'ts_close',
            '前收盘价': 'ts_pre_close',
            '涨跌额': 'ts_change',
            '涨跌幅': 'ts_pct_chg',
            '成交量': 'ts_vol',
            '成交额': 'ts_amount',
            '换手率': 'ts_turnover_rate',
            '换手率(自由流通股)': 'ts_turnover_rate_f',
            '量比': 'ts_volume_ratio',
            '市盈率': 'ts_pe',
            '滚动市盈率': 'ts_pe_ttm',
            '市净率': 'ts_pb',
            '市销率': 'ts_ps',
            '滚动市销率': 'ts_ps_ttm',
            '股息率': 'ts_dv_ratio',
            '滚动股息率': 'ts_dv_ttm',
            '总股本': 'ts_total_share',
            '流通股本': 'ts_float_share',
            '自由流通股本': 'ts_free_share',
            '总市值': 'ts_total_mv',
            '流通市值': 'ts_circ_mv',
        })
        sql_df.to_sql('ts_daily', self.engine, if_exists='append', index=False)

    def delete_ge_trade_date_ts_daily(self, trade_date):
        """
        删除 大于等于这个日期的数据， 即删除这个时间点之后的数据
        :param trade_date:
        :return:
        """
        delete_sql = (
            f"DELETE FROM ts_daily WHERE date(ts_trade_date)>='{trade_date}';"
        )
        self.execute_sql(delete_sql)

    def get_variable(self, variable_name):
        get_variable_sql = (
            f"SELECT value FROM config WHERE variable_name='{variable_name}';"
        )
        result = self.execute_sql(get_variable_sql)
        if result:
            value = result[0][0]
            return value
        else:
            return None

    def set_variable(self, variable_name, value):
        set_variable_sql = (
            f"INSERT INTO config(variable_name, value, set_time) "
            f"VALUES('{variable_name}', '{value}', now()) "
            f"ON DUPLICATE KEY UPDATE value='{value}', set_time=now();"
        )
        self.execute_sql(set_variable_sql)

    def get_all_ts_code(self, contains_st=None):
        ts_requests.get_all_ts_code(contains_st)
        # todo 把 ts_requests.get_all_ts_code(contains_st) 的 内容 存入 到数据库，从数据库获取
        pass

    def get_ts_name(self, ts_code):
        if not self.ts_code_dict:
            self.get_all_ts_code()
        return self.ts_code_dict.get(ts_code)

    def get_ts_date_daily_basic(self, trade_date):
        sql = (
            f"SELECT * FROM ts_daily  WHERE date(ts_trade_date)='{trade_date}';"
        )
        conn = pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               passwd=self.passwd,
                               db=self.db,
                               connect_timeout=self.connect_timeout)
        df = pd.read_sql(sql, conn)
        conn.close()
        df.rename(columns={'ts_trade_date': '日期',
                           'ts_code': '证券代码',
                           'ts_name': '证券名称',
                           'ts_open': '开盘价',
                           'ts_high': '最高价',
                           'ts_low': '最低价',
                           'ts_close': '收盘价',
                           'ts_pre_close': '前收盘价',
                           'ts_change': '涨跌额',
                           'ts_pct_chg': '涨跌幅',
                           'ts_vol': '成交量',
                           'ts_amount': '成交额',
                           'ts_turnover_rate': '换手率',
                           'ts_turnover_rate_f': '换手率(自由流通股)',
                           'ts_volume_ratio': '量比',
                           'ts_pe': '市盈率',
                           'ts_pe_ttm': '滚动市盈率',
                           'ts_pb': '市净率',
                           'ts_ps': '市销率',
                           'ts_ps_ttm': '滚动市销率',
                           'ts_dv_ratio': '股息率',
                           'ts_dv_ttm': '滚动股息率',
                           'ts_total_share': '总股本',
                           'ts_float_share': '流通股本',
                           'ts_free_share': '自由流通股本',
                           'ts_total_mv': '总市值',
                           'ts_circ_mv': '流通市值'
                           }, inplace=True)
        return df

    def get_ts_code_daily(self, ts_code, start_date, end_date):
        sql = (
            ""
        )
        self.execute_sql(sql)

        return


ts_mysql = TsMySQL()


if __name__ == '__main__':
    from core.datas.ts_datas import ts_datas
    from config.config import Config, UserConfig

    ts_datas.init_data_table()
    for _trade_date in ts_datas.get_trade_cal(UserConfig.Start, UserConfig.End):
        printf(_trade_date)
        ts_datas.update_ts_daily_data(_trade_date)

